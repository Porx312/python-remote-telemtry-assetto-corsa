import socket
import time
import struct
import threading
import os
import re
import select
from dotenv import load_dotenv
from ac_packet import ACSP, PacketParser
from database import save_driver, save_lap, init_db, save_touge_battle, start_touge_battle, update_touge_score
from battle_engine import BattleManager
import math

load_dotenv()

SERVERS_PATH = os.getenv('SERVERS_PATH', r'C:\Program Files (x86)\Steam\steamapps\common\assettocorsa\server\cfg')
SERVER_IP = "127.0.0.1"

class DriverInfo:
    def __init__(self, name="", guid="", model=""):
        self.name = name
        self.guid = guid
        self.model = model
        self.best_lap = 0
        self.last_lap = 0
        self.lap_count = 0
        self.current_lap_start = None

class ServerState:
    def __init__(self, listen_port, server_cmd_port, track="Unknown", config="", server_name="Unknown", is_time_attack=True, is_battle=True):
        self.port = listen_port          # Puerto donde Python escucha (UDP_PLUGIN_ADDRESS port)
        self.server_cmd_port = server_cmd_port  # Puerto del servidor para comandos (UDP_PLUGIN_LOCAL_PORT)
        self.track = track
        self.config = config
        self.server_name = server_name
        self.is_time_attack = is_time_attack
        self.is_battle = is_battle
        self.active_drivers = {}
        self.guid_to_driver = {}
        self.last_server_addr = None
        self.sock = None
        self.battle_manager = BattleManager()
        
        # Callback to restart the server session (returns everyone to pits)
        self.battle_manager.on_session_restart = self.restart_session
        
        # Wire DB callbacks so battles are saved live to DB
        self.battle_manager.on_battle_start = lambda p1, p2: start_touge_battle(
            self.server_name, self.track, self.config, p1, p2,
            self.guid_to_driver.get(p1).model if self.guid_to_driver.get(p1) else "Unknown",
            self.guid_to_driver.get(p2).model if self.guid_to_driver.get(p2) else "Unknown"
        )
        self.battle_manager.on_score_update = update_touge_score

    def get_car_id_by_guid(self, guid):
        for cid, driver in self.active_drivers.items():
            if driver.guid == guid:
                return cid
        return None

    def restart_session(self):
        """Envía el comando ACSP_RESTART_SESSION (208) al servidor para regresar a todos a los pits"""
        if not self.last_server_addr or not self.sock:
            return
            
        try:
            # ACSP_RESTART_SESSION = 208
            packet = struct.pack('B', 208)
            self.sock.sendto(packet, self.last_server_addr)
            print(f"🔄 [{self.port}] Executing Session Restart (Teleporting all to pits).")
        except Exception as e:
            print(f"❌ Error sending restart_session: {e}")

def load_server_configs():
    servers = {}
    
    def get_paths(env_var):
        val = os.getenv(env_var, '')
        return [p.strip() for p in val.split(',') if p.strip()]

    legacy_paths = get_paths('SERVERS_PATH')
    ta_paths = get_paths('TIME_ATTACK_SERVERS_PATH')
    battle_paths = get_paths('BATTLE_SERVERS_PATH')
    
    path_roles = {}
    
    # If no specific roles defined, fallback to legacy
    if not ta_paths and not battle_paths:
        for p in legacy_paths:
            path_roles[p] = {'time_attack': True, 'battle': True}
    else:
        for p in legacy_paths:
            path_roles[p] = {'time_attack': True, 'battle': True}
        for p in ta_paths:
            if p not in path_roles: path_roles[p] = {'time_attack': False, 'battle': False}
            path_roles[p]['time_attack'] = True
        for p in battle_paths:
            if p not in path_roles: path_roles[p] = {'time_attack': False, 'battle': False}
            path_roles[p]['battle'] = True

    for base_path, roles in path_roles.items():
        cfg_path = os.path.join(base_path, 'server_cfg.ini')
        if not os.path.exists(cfg_path):
            print(f"⚠️ server_cfg.ini not found at: {cfg_path}")
            continue

        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                content = f.read()

            name_match        = re.search(r'^NAME=(.+)$',                 content, re.MULTILINE)
            track_match       = re.search(r'^TRACK=(.+)$',                content, re.MULTILINE)
            config_match      = re.search(r'^CONFIG_TRACK=(.*)$',         content, re.MULTILINE)
            plugin_port_match = re.search(r'^UDP_PLUGIN_LOCAL_PORT=(\d+)$',content, re.MULTILINE)
            plugin_addr_match = re.search(r'^UDP_PLUGIN_ADDRESS=(.+)$',    content, re.MULTILINE)

            server_cmd_port = 12001  # Puerto del servidor para recibir comandos
            if plugin_port_match:
                server_cmd_port = int(plugin_port_match.group(1).strip())

            # El puerto donde nosotros escuchamos eventos viene de UDP_PLUGIN_ADDRESS
            # Formato: "127.0.0.1:13000" -> extraemos 13000
            listen_port = server_cmd_port  # Fallback si no hay UDP_PLUGIN_ADDRESS
            if plugin_addr_match:
                addr_str = plugin_addr_match.group(1).strip()
                if ':' in addr_str:
                    try:
                        listen_port = int(addr_str.split(':')[1])
                    except:
                        pass
            
            server_name = name_match.group(1).strip()  if name_match  else "Unknown"
            track       = track_match.group(1).strip() if track_match else "Unknown"
            config      = config_match.group(1).strip()if config_match else ""

            print(f"📂 Config loaded from: {cfg_path}")
            print(f"   🏷️ Server: {server_name}")
            print(f"   🗺️ Track: {track} ({config})")
            print(f"   📥 Listen (Events) Port: {listen_port}")
            print(f"   📤 Server Command Port: {server_cmd_port}")
            print(f"   ⚙️ Roles: Time Attack={roles['time_attack']}, Battle={roles['battle']}")
            
            servers[listen_port] = ServerState(listen_port, server_cmd_port, track, config, server_name, roles['time_attack'], roles['battle'])
        except Exception as e:
            print(f"❌ Error reading {cfg_path}: {e}")
            
    return servers

def send_registration(server_state, server_ip):
    """Envía la suscripción al servidor. Usamos el server_cmd_port (UDP_PLUGIN_LOCAL_PORT)."""
    sock = server_state.sock
    target = (server_ip, server_state.server_cmd_port)
    print(f"✉️ Registering with {server_ip}:{server_state.server_cmd_port} <- listen on {server_state.port}...")

    # Handshake
    sock.sendto(struct.pack('B', 0), target)
    # Subscribe Update (200) - interval=50ms tells server to send CAR_UPDATE (53) every 50ms
    sock.sendto(struct.pack('<BH', 200, 50), target)
    # Get Session Info (59)
    sock.sendto(struct.pack('B', 59), target)

    # Request Car Info for first 32 slots (staggered)
    def _request():
        for i in range(32):
            packet = struct.pack('BB', 201, i)  # GET_CAR_INFO
            sock.sendto(packet, target)
            time.sleep(0.05)
    
    threading.Thread(target=_request, daemon=True).start()

def main():
    print("=== AC Telemetry Listener (Python) ===")
    init_db()
    
    servers = load_server_configs()
    if not servers:
        print("❌ No valid server configurations found. Exiting.")
        return

    sockets = []
    sock_to_server = {}

    for port, state in servers.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', port))
        state.sock = sock
        sockets.append(sock)
        sock_to_server[sock] = state
        print(f"🚀 Listening on UDP Port {port} for {state.server_name}")

    while True:
        try:
            readable, _, _ = select.select(sockets, [], [])
            for sock in readable:
                data, addr = sock.recvfrom(4096)
                server_state = sock_to_server[sock]
                
                parser = PacketParser(data)
                packet_type = parser.read_uint8()
                
                if packet_type is None: continue

                # Auto-connect logic: record server IP and register commands to server_cmd_port
                server_ip = addr[0]
                if server_state.last_server_addr is None:
                    print(f"🔌 Auto-Connected from server {server_state.server_name} @ {server_ip}")
                    server_state.last_server_addr = (server_ip, server_state.server_cmd_port)
                    send_registration(server_state, server_ip)

                process_packet(server_state, packet_type, parser, addr)
                
        except KeyboardInterrupt:
            print("Stopping...")
            break
        except Exception as e:
            print(f"❌ Error in main loop: {e}")
            import traceback
            traceback.print_exc()

def process_packet(server_state, packet_type, parser, addr):
    # ==========================================
    # NEW_SESSION (50)
    # ==========================================
    if packet_type == ACSP.NEW_SESSION:  # 50
        parser.read_uint8()  # version
        parser.read_uint8()  # sessionIndex
        parser.read_uint8()  # currentSessionIndex
        parser.read_uint8()  # sessionCount
        
        server_state.server_name = parser.read_wstring()
        _track_from_udp = parser.read_string()
        _config_from_udp = parser.read_string()
        name = parser.read_string()
        
        # When a NEW_SESSION occurs, the track configuration could have changed.
        # So we read the INI file associated with this server port again.
        def get_paths(env_var):
            val = os.getenv(env_var, '')
            return [p.strip() for p in val.split(',') if p.strip()]
        
        paths = get_paths('SERVERS_PATH') + get_paths('TIME_ATTACK_SERVERS_PATH') + get_paths('BATTLE_SERVERS_PATH')
        for base_path in paths:
            cfg_path = os.path.join(base_path, 'server_cfg.ini')
            if os.path.exists(cfg_path):
                try:
                    with open(cfg_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # We first find if this file belongs to THIS port.
                    plugin_port_match = re.search(r'^UDP_PLUGIN_LOCAL_PORT=(\d+)$', content, re.MULTILINE)
                    if plugin_port_match and int(plugin_port_match.group(1).strip()) == server_state.port:
                        track_match = re.search(r'^TRACK=(.+)$', content, re.MULTILINE)
                        config_match = re.search(r'^CONFIG_TRACK=(.*)$', content, re.MULTILINE)
                        
                        if track_match:
                            server_state.track = track_match.group(1).strip()
                        if config_match:
                            server_state.config = config_match.group(1).strip()
                            
                        print(f"🔄 [{server_state.port}] Reloaded Config: {server_state.track} ({server_state.config})")
                        break
                except Exception as e:
                    print(f"❌ Error reloading {cfg_path}: {e}")

        print(f"🌍 Session [{server_state.port}]: {server_state.track} ({server_state.config}) | Server: {server_state.server_name}")

    # ==========================================
    # NEW_CONNECTION (51)
    # ==========================================
    elif packet_type == ACSP.NEW_CONNECTION:  # 51
        name = parser.read_wstring()
        guid = parser.read_wstring()
        car_id = parser.read_uint8()
        if car_id is None: return
        model = parser.read_string()
        skin = parser.read_string()

        if not name or not guid:
            return

        old_driver = server_state.active_drivers.get(car_id)

        if old_driver and old_driver.guid != guid:
            print(f"🔄 [{server_state.port}] CarID {car_id} changed: {old_driver.name} -> {name}")
            if old_driver.guid in server_state.guid_to_driver:
                del server_state.guid_to_driver[old_driver.guid]
            old_driver = None

        if old_driver:
            if old_driver.model != model and model != "":
                print(f"✨ [{server_state.port}] Car Change for CarID {car_id}: {old_driver.model} -> {model}")
                old_driver.best_lap = 0
                old_driver.lap_count = 0
            old_driver.name = name
            old_driver.guid = guid
            old_driver.model = model
            driver = old_driver
        else:
            driver = DriverInfo(name, guid, model)
            server_state.active_drivers[car_id] = driver

        if guid and not guid.startswith('unknown_'):
            server_state.guid_to_driver[guid] = driver

        print(f"🟢 [{server_state.port}] [CONNECTED] CarID {car_id} | {name} | {model} | {guid}")
        save_driver(guid, name, model)

    # ==========================================
    # CAR_INFO (210)
    # ==========================================
    elif packet_type == ACSP.CAR_INFO:  # 210
        car_id = parser.read_uint8()
        if car_id is None: return
        is_connected = parser.read_uint8()

        model = parser.read_wstring()
        skin = parser.read_wstring()
        name = parser.read_wstring()
        team = parser.read_wstring()
        guid = parser.read_wstring()

        if not name or not guid or is_connected == 0:
            return

        old_driver = server_state.active_drivers.get(car_id)

        if old_driver and old_driver.guid != guid:
            if old_driver.guid in server_state.guid_to_driver:
                del server_state.guid_to_driver[old_driver.guid]
            old_driver = None

        if old_driver:
            if old_driver.model != model and model != "":
                print(f"✨ [{server_state.port}] Car Change for CarID {car_id}: {old_driver.model} -> {model}")
                old_driver.best_lap = 0
                old_driver.lap_count = 0
            old_driver.name = name
            old_driver.guid = guid
            old_driver.model = model
            driver = old_driver
        else:
            driver = DriverInfo(name, guid, model)
            server_state.active_drivers[car_id] = driver

        if guid and not guid.startswith('unknown_'):
            server_state.guid_to_driver[guid] = driver

        print(f"🏎️ [{server_state.port}] [CAR_INFO] CarID {car_id} | {name} | {model} | {guid}")
        save_driver(guid, name, model)

    # ==========================================
    # CAR_UPDATE (54)
    # ==========================================
    elif packet_type == ACSP.CAR_UPDATE:  # 54
        car_id = parser.read_uint8()
        if car_id is None: return
        pos_x = parser.read_float()
        pos_y = parser.read_float()
        pos_z = parser.read_float()
        v_x = parser.read_float()
        v_y = parser.read_float()
        v_z = parser.read_float()
        gear = parser.read_uint8()
        rpm = parser.read_uint16()
        spline = parser.read_float()
        
        if spline is None: return
        
        driver = server_state.active_drivers.get(car_id)
        if not driver or not driver.guid or driver.guid.startswith('unknown_'):
            return
            
        v_x = v_x or 0.0
        v_y = v_y or 0.0
        v_z = v_z or 0.0
        
        speed_ms = math.sqrt(v_x**2 + v_y**2 + v_z**2)
        speedKmh = speed_ms * 3.6
        pos = (pos_x, pos_y, pos_z)
        
        if server_state.is_battle:
            server_state.battle_manager.update(driver.guid, spline, speedKmh, pos)

    # ==========================================
    # CONNECTION_CLOSED (52)
    # ==========================================
    elif packet_type == ACSP.CONNECTION_CLOSED:  # 52
        name = parser.read_wstring()
        guid = parser.read_wstring()
        car_id = parser.read_uint8()
        if car_id is None: return

        driver = server_state.active_drivers.get(car_id)
        if driver:
            print(f"👋 [{server_state.port}] Disconnected: {driver.name} (CarID {car_id}, SteamID: {driver.guid})")
            if driver.best_lap > 0 and not driver.guid.startswith('unknown_') and server_state.is_time_attack:
                now = int(time.time() * 1000)
                save_lap(driver.guid, driver.model, server_state.track, server_state.config, server_state.server_name, driver.best_lap, True, now)
            
            # Remover del motor de batallas
            if server_state.is_battle:
                server_state.battle_manager.remove_car(driver.guid)
            
            if driver.guid in server_state.guid_to_driver:
                del server_state.guid_to_driver[driver.guid]
            del server_state.active_drivers[car_id]
        else:
            print(f"👋 [{server_state.port}] Disconnected: {name} (CarID {car_id}, not tracked)")

    # ==========================================
    # LAP_COMPLETED (58)
    # ==========================================
    elif packet_type == ACSP.LAP_COMPLETED:  # 58
        car_id = parser.read_uint8()
        if car_id is None: return

        ac_lap_time = parser.read_uint32() or 0
        cuts = parser.read_uint8() or 0

        now = int(time.time() * 1000)
        driver = server_state.active_drivers.get(car_id)

        if not driver:
            driver = DriverInfo(f"Driver_CarID_{car_id}", f"unknown_{car_id}", "Unknown")
            server_state.active_drivers[car_id] = driver
            print(f"❓ [{server_state.port}] Unknown CarID {car_id}. Requesting info...")
            if server_state.last_server_addr:
                server_state.sock.sendto(struct.pack('BB', 201, car_id), server_state.last_server_addr)

        if ac_lap_time <= 0 or ac_lap_time > 36000000:
            return

        driver.last_lap = ac_lap_time
        driver.lap_count += 1
        is_valid = (cuts == 0)

        if is_valid:
            if driver.best_lap == 0 or ac_lap_time < driver.best_lap:
                driver.best_lap = ac_lap_time

            print(f"🏁 [{server_state.port}] [LAP] ✅ | {driver.name} | Time: {ac_lap_time/1000:.3f}s | Best: {driver.best_lap/1000:.3f}s | Cuts: {cuts}")

            if not server_state.is_time_attack:
                print(f"   ⏳ Skipping DB save (Server configured as Battle Only)")
            elif driver.guid.startswith('unknown_'):
                print(f"   ⏳ Skipping DB save (unknown driver)")
            else:
                print(f"   💾 Saving -> SteamID: {driver.guid} | Car: {driver.model} | Track: {server_state.track} Route: {server_state.config} | Server: {server_state.server_name}")
                save_lap(driver.guid, driver.model, server_state.track, server_state.config, server_state.server_name, ac_lap_time, True, now)
        else:
            print(f"🏁 [{server_state.port}] [LAP] ⚠️ INVALID | {driver.name} | Time: {ac_lap_time/1000:.3f}s | Cuts: {cuts}")

    # ==========================================
    # CLIENT_EVENT (130)
    # ==========================================
    elif packet_type == ACSP.CLIENT_EVENT:  # 130
        ev_type = parser.read_uint8()
        car_id = parser.read_uint8()
        
        if server_state.is_battle and ev_type == ACSP.CE_COLLISION_WITH_CAR:
            other_car_id = parser.read_uint8()
            impact_speed = parser.read_float()
            
            # Get driver GUIDs
            driver1 = server_state.active_drivers.get(car_id)
            driver2 = server_state.active_drivers.get(other_car_id)
            
            if driver1 and driver2 and hasattr(driver1, 'guid') and hasattr(driver2, 'guid'):
                if not driver1.guid.startswith('unknown_') and not driver2.guid.startswith('unknown_'):
                    server_state.battle_manager.handle_collision(driver1.guid, driver2.guid, impact_speed)

if __name__ == "__main__":
    main()


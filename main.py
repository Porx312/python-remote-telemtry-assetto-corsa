"""
ac-telemetry-events / main.py
==============================
Dedicated Assetto Corsa telemetry listener for event servers.
Reads the active event configuration from the `server_events` DB table and
dynamically dispatches the correct webhook payload when laps are completed.

Supports any event type registered in the DB:
  - endurance_progress
  - time_attack          (add payload builder in event_dispatcher.py)
  - drift_score          (add payload builder in event_dispatcher.py)

Environment variables (.env):
  SERVERS_PATH            = Legacy base path for server configs
  TIME_ATTACK_SERVERS_PATH = Paths for time-attack servers (comma-separated)
  EVENTS_SERVERS_PATH      = Paths for event servers (comma-separated)  ← NEW
  API_KEY                  = API Key to authenticate against the backend
  BACKEND_URL              = Base URL of the Node.js backend (not used here, reserved)
"""

import socket
import time
import struct
import threading
import os
import re
import select
import requests
from dotenv import load_dotenv
from ac_packet import ACSP, PacketParser
from database import save_driver, save_lap, init_db, get_active_server_event
from event_dispatcher import dispatch_event
import math

load_dotenv()

SERVERS_PATH = os.getenv('SERVERS_PATH', '')
SERVER_IP    = "127.0.0.1"


# ──────────────────────────────────────────────
# DATA CLASSES
# ──────────────────────────────────────────────

class DriverInfo:
    def __init__(self, name="", guid="", model=""):
        self.name        = name
        self.guid        = guid
        self.model       = model
        self.best_lap    = 0
        self.last_lap    = 0
        self.lap_count   = 0
        self.failed_laps = 0
        
        # Idle detection
        self.last_pos         = None
        self.last_pos_time    = 0
        
        # Restart constraint detection
        self.lap_start_time   = 0
        
        # Real-time state tracking
        self.lap_notified_fail= False


class ServerState:
    def __init__(self, listen_port, server_cmd_port, track="Unknown", config="", server_name="Unknown"):
        self.port             = listen_port
        self.server_cmd_port  = server_cmd_port
        self.track            = track
        self.config           = config
        self.server_name      = server_name
        self.config_server_name = server_name  # Original name from .ini, never overwritten
        self.active_drivers   = {}
        self.guid_to_driver   = {}
        self.last_server_addr = None
        self.sock             = None


# ──────────────────────────────────────────────
# PACKET PROCESSING
# ──────────────────────────────────────────────

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

def send_chat(server_state, car_id, message):
    """Envía un mensaje privado al chat del jugador en el servidor."""
    if not server_state.last_server_addr: return
    try:
        sock = server_state.sock
        target = (server_state.last_server_addr[0], server_state.server_cmd_port)
        
        # SEND_CHAT (202) formato: [byte id] [byte car_id] [byte len] [wstring mensaje]
        msg_bytes = message.encode('utf-32le', errors='replace')
        num_chars = len(msg_bytes) // 4
        
        # 202, car_id, length (in utf-32 chars)
        header = struct.pack('BBB', 202, car_id, num_chars)
        sock.sendto(header + msg_bytes, target)
    except Exception as e:
        print(f"❌ Error enviando chat a Car{car_id}: {e}")

def send_admin_command(server_state, command_str):
    """Envía un comando de administrador (ej: /pit <id>, /kick <id>) al servidor."""
    if not server_state.last_server_addr: return
    try:
        sock = server_state.sock
        target = (server_state.last_server_addr[0], server_state.server_cmd_port)
        
        # ACSP ADMIN_COMMAND (208) formato: [byte id] [byte len] [wstring command]
        cmd_bytes = command_str.encode('utf-32le', errors='replace')
        num_chars = len(cmd_bytes) // 4
        
        header = struct.pack('BB', getattr(ACSP, 'ADMIN_COMMAND', 208), num_chars)
        sock.sendto(header + cmd_bytes, target)
        print(f"🔨 [{server_state.port}] AdminCmd: {command_str}")
    except Exception as e:
        print(f"❌ Error enviando AdminCmd '{command_str}': {e}")

def process_packet(data, server_state, addr):
    # Auto-connect logic: register once per server startup/connection when we see traffic
    server_ip = addr[0]
    if server_state.last_server_addr is None:
        print(f"🔌 Auto-Connected from server {server_state.server_name} @ {server_ip}")
        server_state.last_server_addr = (server_ip, server_state.server_cmd_port)
        send_registration(server_state, server_ip)

    server_state.last_server_addr = addr
    parser = PacketParser(data)
    packet_type = parser.read_uint8()
    if packet_type is None:
        return

    # ─── NEW_SESSION (50) ───────────────────────────────────
    if packet_type == ACSP.NEW_SESSION:
        parser.read_uint8()   # version
        parser.read_uint8()   # sessionIndex
        parser.read_uint8()   # currentSessionIndex
        parser.read_uint8()   # sessionCount

        server_state.server_name = parser.read_wstring()
        _track  = parser.read_string()
        _config = parser.read_string()

        def get_paths(env_var):
            val = os.getenv(env_var, '')
            return [p.strip() for p in val.split(',') if p.strip()]

        paths = get_paths('SERVERS_PATH') + get_paths('TIME_ATTACK_SERVERS_PATH') + get_paths('EVENTS_SERVERS_PATH')

        for base_path in paths:
            cfg_path = os.path.join(base_path, 'server_cfg.ini')
            if os.path.exists(cfg_path):
                try:
                    with open(cfg_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    port_match = re.search(r'^UDP_PLUGIN_LOCAL_PORT=(\d+)$', content, re.MULTILINE)
                    if port_match and int(port_match.group(1).strip()) == server_state.port:
                        track_m  = re.search(r'^TRACK=(.+)$', content, re.MULTILINE)
                        config_m = re.search(r'^CONFIG_TRACK=(.*)$', content, re.MULTILINE)
                        if track_m:
                            server_state.track = track_m.group(1).strip()
                        if config_m:
                            server_state.config = config_m.group(1).strip()
                        print(f"🔄 [{server_state.port}] Config reloaded: {server_state.track} ({server_state.config})")
                        break
                except Exception as e:
                    print(f"❌ Error reloading {cfg_path}: {e}")

        # Log active event for this server (try session name, then config name)
        print(f"   🔍 Buscando evento: session='{server_state.server_name}' | config='{server_state.config_server_name}'")
        event = get_active_server_event(server_state.server_name)
        if not event:
            event = get_active_server_event(server_state.config_server_name)
        event_info = f" | 🎮 Event: {event['event_type']}" if event else " | ⚠️  No Event registered"
        print(f"🌍 Session [{server_state.port}]: {server_state.track} ({server_state.config}) | {server_state.server_name}{event_info}")

    # ─── NEW_CONNECTION (51) ────────────────────────────────
    elif packet_type == ACSP.NEW_CONNECTION:
        name   = parser.read_wstring()
        guid   = parser.read_wstring()
        car_id = parser.read_uint8()
        if car_id is None: return
        model  = parser.read_string()
        _skin  = parser.read_string()

        if not name or not guid: return

        driver = DriverInfo(name, guid, model)
        server_state.active_drivers[car_id] = driver
        if guid and not guid.startswith('unknown_'):
            server_state.guid_to_driver[guid] = driver

        print(f"🟢 [{server_state.port}] [CONNECTED] CarID {car_id} | {name} | {model} | {guid}")
        save_driver(guid, name, model)

        driver.lap_start_time = time.time() * 1000
        driver.lap_notified_fail = False

        # Notify: player is now active (isStillGoing=True)
        if not guid.startswith('unknown_'):
            dispatch_event(server_state, driver, lap_time_ms=0, is_finished=False)

    # ─── CAR_INFO (210) ─────────────────────────────────────
    elif packet_type == ACSP.CAR_INFO:
        car_id       = parser.read_uint8()
        if car_id is None: return
        is_connected = parser.read_uint8()
        model   = parser.read_wstring()
        _skin   = parser.read_wstring()
        name    = parser.read_wstring()
        _team   = parser.read_wstring()
        guid    = parser.read_wstring()

        if not name or not guid or is_connected == 0: return

        driver = DriverInfo(name, guid, model)
        server_state.active_drivers[car_id] = driver
        if guid and not guid.startswith('unknown_'):
            server_state.guid_to_driver[guid] = driver

        print(f"🏎️ [{server_state.port}] [CAR_INFO] CarID {car_id} | {name} | {model} | {guid}")
        save_driver(guid, name, model)

    # ─── CONNECTION_CLOSED (52) ─────────────────────────────
    elif packet_type == ACSP.CONNECTION_CLOSED:
        name   = parser.read_wstring()
        guid   = parser.read_wstring()
        car_id = parser.read_uint8()
        if car_id is None: return

        driver = server_state.active_drivers.get(car_id)
        if driver:
            print(f"👋 [{server_state.port}] Disconnected: {driver.name} (CarID {car_id})")
            # Notify: player disconnected (isStillGoing=False)
            if not driver.guid.startswith('unknown_'):
                dispatch_event(server_state, driver, driver.last_lap, is_finished=True)
            if driver.guid in server_state.guid_to_driver:
                del server_state.guid_to_driver[driver.guid]
            del server_state.active_drivers[car_id]

    # ─── CAR_UPDATE (53) ────────────────────────────────────
    elif packet_type == getattr(ACSP, 'CAR_UPDATE', 53):
        car_id = parser.read_uint8()
        if car_id is None: return
        pos_x  = parser.read_float()
        pos_y  = parser.read_float()
        pos_z  = parser.read_float()
        v_x    = parser.read_float()
        v_y    = parser.read_float()
        v_z    = parser.read_float()
        gear   = parser.read_uint8()
        rpm    = parser.read_uint16()
        spline = parser.read_float()
        
        driver = server_state.active_drivers.get(car_id)
        if driver:
            speed_ms = ((v_x or 0)**2 + (v_y or 0)**2 + (v_z or 0)**2)**0.5
            now = time.time() * 1000
            if speed_ms < 0.5: # 0.5 m/s (~ 1.8 km/h) threshold for idle
                if driver.last_pos_time == 0:
                    driver.last_pos_time = now
                elif (now - driver.last_pos_time) > 5000: # Over 5 seconds idle
                    driver.was_idle = True
                    event = get_active_server_event(server_state.server_name) or get_active_server_event(server_state.config_server_name)
                    meta = event.get("metadata", {}) if event else {}
                    # Only notify once per incident, and only test idle if they actually completed 1 lap (lap_count > 0, means starting 2nd lap or later depending on logic, let's just make it lap_count >= 1 so 1st completed lap)
                    if meta.get("detectIdle", False) and driver.lap_count >= 1 and not getattr(driver, "idle_notified", False):
                        driver.idle_notified = True
                        driver.failed_laps = getattr(driver, 'failed_laps', 0) + 1
                        driver.lap_notified_fail = True
                        max_fails = meta.get("maxFails", "?")
                        send_chat(server_state, car_id, f"[EVENT] Lap FAILED: Stopped on track (>5s) ({driver.failed_laps}/{max_fails} fails)")
                        send_admin_command(server_state, f"/pit {car_id}")
            else:
                driver.last_pos_time = 0

    # ─── CLIENT_EVENT (130) ─────────────────────────────────
    elif packet_type == getattr(ACSP, 'CLIENT_EVENT', 130):
        ev_type = parser.read_uint8()
        car_id  = parser.read_uint8()
        if ev_type in (getattr(ACSP, 'CE_COLLISION_WITH_CAR', 10), getattr(ACSP, 'CE_COLLISION_WITH_ENV', 11)):
            driver = server_state.active_drivers.get(car_id)
            if driver:
                driver.had_collision = True
                event = get_active_server_event(server_state.server_name) or get_active_server_event(server_state.config_server_name)
                meta = event.get("metadata", {}) if event else {}
                if meta.get("enableCollisions", False) and not getattr(driver, "collision_notified", False):
                    driver.collision_notified = True
                    driver.failed_laps = getattr(driver, 'failed_laps', 0) + 1
                    driver.lap_notified_fail = True
                    max_fails = meta.get("maxFails", "?")
                    send_chat(server_state, car_id, f"[EVENT] Lap FAILED: Collision ({driver.failed_laps}/{max_fails} fails)")
                    send_admin_command(server_state, f"/pit {car_id}")

    # ─── LAP_COMPLETED (58) ─────────────────────────────────
    elif packet_type == ACSP.LAP_COMPLETED:
        car_id      = parser.read_uint8()
        if car_id is None: return
        ac_lap_time = parser.read_uint32() or 0
        cuts        = parser.read_uint8() or 0

        now    = int(time.time() * 1000)
        driver = server_state.active_drivers.get(car_id)

        if not driver:
            driver = DriverInfo(f"Driver_CarID_{car_id}", f"unknown_{car_id}", "Unknown")
            server_state.active_drivers[car_id] = driver
            if server_state.last_server_addr:
                server_state.sock.sendto(struct.pack('BB', 201, car_id), server_state.last_server_addr)

        if ac_lap_time <= 0 or ac_lap_time > 36000000:
            return

        driver.last_lap   = ac_lap_time
        driver.lap_count += 1
        is_valid = (cuts == 0)

        # Get active event settings to check constraints
        event = get_active_server_event(server_state.server_name)
        if not event:
            event = get_active_server_event(server_state.config_server_name)

        meta = event.get("metadata", {}) if event else {}
        
        fail_reason = ""

        if meta.get("enableCollisions", False) and getattr(driver, "had_collision", False):
            is_valid = False
            fail_reason = "Collision"

        if meta.get("detectIdle", False) and getattr(driver, "was_idle", False) and driver.lap_count >= 1:
            is_valid = False
            fail_reason = "Stopped on track (>5s)"

        # Set Restart Check using standard AC time metrics:
        # If AC detects cuts > 0 (e.g. they teleported to pits or went out of track), track it here.
        # Note: If cuts > 0 and lap time is abnormally small, usually a teleport. AC marks cuts naturally.
        if cuts > 0:
            is_valid = False
            fail_reason = "Track Cut / Teleport"

        total_laps = meta.get("totalLaps", "?")
        max_fails  = meta.get("maxFails", "?")
        was_notified = getattr(driver, 'lap_notified_fail', False)

        # Reset real-time tracking constraints for the next lap
        driver.lap_start_time = now
        driver.had_collision  = False
        driver.restarted_lap  = False
        driver.was_idle       = False
        driver.lap_notified_fail  = False
        driver.collision_notified = False
        driver.idle_notified      = False

        if not is_valid:
            if not was_notified:
                driver.failed_laps = getattr(driver, 'failed_laps', 0) + 1
                fail_reason_display = fail_reason if fail_reason else "Track Cut"
                send_chat(server_state, car_id, f"[EVENT] Lap FAILED: {fail_reason_display} ({driver.failed_laps}/{max_fails} fails)")
                # If they already teleported (Track Cut / Teleport), we might not need to send /pit, but it's safe to enforce it.
                send_admin_command(server_state, f"/pit {car_id}")
            
            print(f"🏁 [{server_state.port}] [LAP] ⚠️  INVALID | {driver.name} | {ac_lap_time/1000:.3f}s | Cuts: {cuts}")
            return

        if driver.best_lap == 0 or ac_lap_time < driver.best_lap:
            driver.best_lap = ac_lap_time

        print(f"🏁 [{server_state.port}] [LAP] ✅ | {driver.name} | Lap #{driver.lap_count} | {ac_lap_time/1000:.3f}s | Best: {driver.best_lap/1000:.3f}s")
        send_chat(server_state, car_id, f"[EVENT] Lap {driver.lap_count}/{total_laps} COMPLETED! Time: {ac_lap_time/1000:.3f}s")

        if not driver.guid.startswith('unknown_'):
            save_lap(driver.guid, driver.model, server_state.track, server_state.config,
                     server_state.server_name, ac_lap_time, True, now)

        # ── Dispatch dynamic webhook based on active event ──
        dispatch_event(server_state, driver, lap_time_ms=ac_lap_time)


# ──────────────────────────────────────────────
# SERVER LISTENER THREAD
# ──────────────────────────────────────────────

def listen_server(server_state):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_IP, server_state.port))
    sock.setblocking(False)
    server_state.sock = sock

    print(f"🎧 Events listener started on port {server_state.port} — Server: {server_state.server_name}")

    while True:
        ready = select.select([sock], [], [], 0.5)
        if ready[0]:
            try:
                data, addr = sock.recvfrom(4096)
                process_packet(data, server_state, addr)
            except Exception as e:
                print(f"❌ [{server_state.port}] Packet error: {e}")


# ──────────────────────────────────────────────
# LOAD SERVER CONFIGS
# ──────────────────────────────────────────────

def load_server_configs():
    servers = {}

    def get_paths(env_var):
        val = os.getenv(env_var, '').strip('"').strip("'")
        return [p.strip().strip('"').strip("'") for p in val.split(',') if p.strip()]

    # Combine all configured server paths
    all_paths = get_paths('SERVERS_PATH') + get_paths('TIME_ATTACK_SERVERS_PATH') + get_paths('EVENTS_SERVERS_PATH')

    for base_path in all_paths:
        cfg_path = os.path.join(base_path, 'server_cfg.ini')
        if not os.path.exists(cfg_path):
            print(f"⚠️  server_cfg.ini not found at: {cfg_path}")
            continue

        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                content = f.read()

            plugin_port_m  = re.search(r'^UDP_PLUGIN_LOCAL_PORT=(\d+)$', content, re.MULTILINE)
            # UDP_PLUGIN_ADDRESS can be either "host:port" or just a port number
            udp_addr_m     = re.search(r'^UDP_PLUGIN_ADDRESS=(?:[^:]+:)?(\d+)$', content, re.MULTILINE)
            server_name_m  = re.search(r'^SERVER_NAME=(.+)$', content, re.MULTILINE)
            track_m        = re.search(r'^TRACK=(.+)$', content, re.MULTILINE)
            config_m       = re.search(r'^CONFIG_TRACK=(.*)$', content, re.MULTILINE)

            if not plugin_port_m or not udp_addr_m:
                print(f"⚠️  Missing UDP ports in {cfg_path}")
                continue

            cmd_port    = int(plugin_port_m.group(1).strip())
            listen_port = int(udp_addr_m.group(1).strip())
            name        = server_name_m.group(1).strip() if server_name_m else "Events Server"
            track       = track_m.group(1).strip()       if track_m       else "Unknown"
            config      = config_m.group(1).strip()      if config_m      else ""

            if listen_port not in servers:
                servers[listen_port] = ServerState(listen_port, cmd_port, track, config, name)
                print(f"📋 Events server loaded: {name} | {track} ({config}) | Listen:{listen_port}")

        except Exception as e:
            print(f"❌ Error reading {cfg_path}: {e}")

    return servers


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    init_db()
    servers = load_server_configs()

    if not servers:
        print("❌ No event server configurations found. Check EVENTS_SERVERS_PATH in .env")
        return

    threads = []
    for server_state in servers.values():
        t = threading.Thread(target=listen_server, args=(server_state,), daemon=True)
        t.start()
        threads.append(t)

    print(f"\n✅ {len(servers)} event server(s) running. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n👋 Stopping event servers.")

if __name__ == "__main__":
    main()

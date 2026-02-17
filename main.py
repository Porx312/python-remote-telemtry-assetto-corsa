import socket
import time
import struct
import threading
import os
import re
from dotenv import load_dotenv
from ac_packet import ACSP, PacketParser
from database import save_driver, save_lap, init_db

load_dotenv()

SERVERS_PATH = os.getenv('SERVERS_PATH', r'C:\Program Files (x86)\Steam\steamapps\common\assettocorsa\server\cfg')
LISTEN_PORT = 12000
SERVER_IP = "127.0.0.1"

class DriverInfo:
    def __init__(self, name="", guid="", model=""):
        self.name = name
        self.guid = guid
        self.model = model
        self.best_lap = 0
        self.last_lap = 0
        self.lap_count = 0
        self.current_lap_start = None  # timestamp in ms (manual timing)

active_drivers = {}  # Slot ID -> DriverInfo
guid_to_driver = {}  # SteamID -> DriverInfo (secondary index)
last_server_addr = None  # (ip, port)
current_track = "Unknown"
current_server = "Unknown"
current_config = ""

def load_server_config():
    """Read track, server name, and config from server_cfg.ini"""
    global current_track, current_server, current_config, LISTEN_PORT

    cfg_path = os.path.join(SERVERS_PATH, 'server_cfg.ini')
    if not os.path.exists(cfg_path):
        print(f"⚠️ server_cfg.ini not found at: {cfg_path}")
        return

    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            content = f.read()

        name_match = re.search(r'^NAME=(.+)$', content, re.MULTILINE)
        track_match = re.search(r'^TRACK=(.+)$', content, re.MULTILINE)
        config_match = re.search(r'^CONFIG_TRACK=(.*)$', content, re.MULTILINE)
        plugin_port_match = re.search(r'^UDP_PLUGIN_LOCAL_PORT=(\d+)$', content, re.MULTILINE)

        if name_match:
            current_server = name_match.group(1).strip()
        if track_match:
            current_track = track_match.group(1).strip()
        if config_match:
            current_config = config_match.group(1).strip()
        if plugin_port_match:
            LISTEN_PORT = int(plugin_port_match.group(1).strip())

        print(f"📂 Config loaded from: {cfg_path}")
        print(f"   🏷️ Server: {current_server}")
        print(f"   🗺️ Track: {current_track} ({current_config})")
        print(f"   🔌 Plugin Port: {LISTEN_PORT}")
    except Exception as e:
        print(f"❌ Error reading server_cfg.ini: {e}")

def send_registration(sock, ip, port):
    global last_server_addr
    last_server_addr = (ip, port)
    print(f"✉️ Registering with {ip}:{port}...")

    # Handshake
    sock.sendto(struct.pack('B', 0), (ip, port))
    # Subscribe Update (200)
    sock.sendto(struct.pack('B', 200), (ip, port))
    # Subscribe Spot (201)
    sock.sendto(struct.pack('B', 201), (ip, port))
    # Get Session Info (59)
    sock.sendto(struct.pack('B', 59), (ip, port))

    # Request Car Info for first 32 slots (staggered)
    def _request():
        for i in range(32):
            packet = struct.pack('BB', 201, i)  # GET_CAR_INFO
            sock.sendto(packet, (ip, port))
            time.sleep(0.05)
    
    threading.Thread(target=_request, daemon=True).start()

def main():
    print("=== AC Telemetry Listener (Python) ===")
    load_server_config()
    init_db()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', LISTEN_PORT))
    print(f"🚀 Listening on UDP Port {LISTEN_PORT}")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            parser = PacketParser(data)
            packet_type = parser.read_uint8()
            
            if packet_type is None: continue

            # Auto-connect logic
            global last_server_addr
            if last_server_addr is None or last_server_addr != addr:
                print(f"🔌 Auto-Connected to server at {addr[0]}:{addr[1]}")
                send_registration(sock, addr[0], addr[1])

            # ==========================================
            # NEW_SESSION (50)
            # ==========================================
            if packet_type == ACSP.NEW_SESSION:  # 50
                parser.read_uint8()  # version
                parser.read_uint8()  # sessionIndex
                parser.read_uint8()  # currentSessionIndex
                parser.read_uint8()  # sessionCount
                
                global current_server, current_track
                current_server = parser.read_string()
                current_track = parser.read_string()
                t_config = parser.read_string()
                name = parser.read_string()
                
                print(f"🌍 Session: {current_track} ({t_config}) | Server: {current_server}")

            # ==========================================
            # NEW_CONNECTION (51)
            # Format: [name][guid][carId][model][skin]
            # ==========================================
            elif packet_type == ACSP.NEW_CONNECTION:  # 51
                name = parser.read_string()
                guid = parser.read_string()
                car_id = parser.read_uint8()
                if car_id is None: continue
                model = parser.read_string()
                skin = parser.read_string()

                if not name or not guid:
                    continue

                old_driver = active_drivers.get(car_id)

                # If a DIFFERENT player takes this carId, create fresh DriverInfo
                # (don't inherit old best_lap/lap_count from previous player!)
                if old_driver and old_driver.guid != guid:
                    print(f"🔄 CarID {car_id} changed: {old_driver.name} -> {name}")
                    # Clean up old driver from guid index
                    if old_driver.guid in guid_to_driver:
                        del guid_to_driver[old_driver.guid]
                    old_driver = None  # Force creation of new DriverInfo

                if old_driver:
                    # Same player reconnecting (e.g. car change)
                    if old_driver.model != model and model != "":
                        print(f"✨ Car Change for CarID {car_id}: {old_driver.model} -> {model}")
                        old_driver.best_lap = 0  # Reset best lap on car change
                        old_driver.lap_count = 0
                    old_driver.name = name
                    old_driver.guid = guid
                    old_driver.model = model
                    driver = old_driver
                else:
                    driver = DriverInfo(name, guid, model)
                    active_drivers[car_id] = driver

                if guid and not guid.startswith('unknown_'):
                    guid_to_driver[guid] = driver

                print(f"🟢 [CONNECTED] CarID {car_id} | {name} | {model} | {guid}")
                save_driver(guid, name, model)

            # ==========================================
            # CAR_INFO (54)
            # Format: [carId][isConnected][model][skin][name][team][guid]
            # ==========================================
            elif packet_type == ACSP.CAR_INFO:  # 54
                car_id = parser.read_uint8()
                if car_id is None: continue
                is_connected = parser.read_uint8()

                model = parser.read_string()
                skin = parser.read_string()
                name = parser.read_string()
                team = parser.read_string()
                guid = parser.read_string()

                if not name or not guid or is_connected == 0:
                    continue

                old_driver = active_drivers.get(car_id)

                # Different player at same carId -> fresh DriverInfo
                if old_driver and old_driver.guid != guid:
                    if old_driver.guid in guid_to_driver:
                        del guid_to_driver[old_driver.guid]
                    old_driver = None

                if old_driver:
                    if old_driver.model != model and model != "":
                        print(f"✨ Car Change for CarID {car_id}: {old_driver.model} -> {model}")
                        old_driver.best_lap = 0
                        old_driver.lap_count = 0
                    old_driver.name = name
                    old_driver.guid = guid
                    old_driver.model = model
                    driver = old_driver
                else:
                    driver = DriverInfo(name, guid, model)
                    active_drivers[car_id] = driver

                if guid and not guid.startswith('unknown_'):
                    guid_to_driver[guid] = driver

                print(f"🏎️ [CAR_INFO] CarID {car_id} | {name} | {model} | {guid}")
                save_driver(guid, name, model)

            # ==========================================
            # CAR_UPDATE (53) - Realtime telemetry, skip
            # ==========================================
            elif packet_type == ACSP.CAR_UPDATE:  # 53
                pass

            # ==========================================
            # CONNECTION_CLOSED (52)
            # Format: [name][guid][carId]
            # ==========================================
            elif packet_type == ACSP.CONNECTION_CLOSED:  # 52
                name = parser.read_string()
                guid = parser.read_string()
                car_id = parser.read_uint8()
                if car_id is None: continue

                driver = active_drivers.get(car_id)
                if driver:
                    print(f"👋 Disconnected: {driver.name} (CarID {car_id}, SteamID: {driver.guid})")
                    if driver.best_lap > 0 and not driver.guid.startswith('unknown_'):
                        now = int(time.time() * 1000)
                        save_lap(driver.guid, driver.model, current_track, current_server, driver.best_lap, True, now)
                    # Remove from secondary index
                    if driver.guid in guid_to_driver:
                        del guid_to_driver[driver.guid]
                    del active_drivers[car_id]
                else:
                    print(f"👋 Disconnected: {name} (CarID {car_id}, not tracked)")

            # ==========================================
            # LAP_COMPLETED (73)
            # Uses acLapTime directly from the server.
            # ==========================================
            elif packet_type == ACSP.LAP_COMPLETED:  # 73
                car_id = parser.read_uint8()
                if car_id is None: continue

                ac_lap_time = parser.read_uint32() or 0
                cuts = parser.read_uint8() or 0

                now = int(time.time() * 1000)
                driver = active_drivers.get(car_id)

                # Unknown driver -> create placeholder, request info
                if not driver:
                    driver = DriverInfo(f"Driver_CarID_{car_id}", f"unknown_{car_id}", "Unknown")
                    active_drivers[car_id] = driver
                    print(f"❓ Unknown CarID {car_id}. Requesting info...")
                    if last_server_addr:
                        sock.sendto(struct.pack('BB', 201, car_id), last_server_addr)

                # Sanity check: ignore garbage times
                if ac_lap_time <= 0 or ac_lap_time > 36000000:
                    continue

                # Update driver stats
                driver.last_lap = ac_lap_time
                driver.lap_count += 1
                is_valid = (cuts == 0)

                if is_valid:
                    if driver.best_lap == 0 or ac_lap_time < driver.best_lap:
                        driver.best_lap = ac_lap_time

                    print(f"🏁 [LAP] ✅ | {driver.name} | Time: {ac_lap_time/1000:.3f}s | Best: {driver.best_lap/1000:.3f}s | Cuts: {cuts}")

                    if driver.guid.startswith('unknown_'):
                        print(f"   ⏳ Skipping DB save (unknown driver)")
                    else:
                        print(f"   💾 Saving -> SteamID: {driver.guid} | Car: {driver.model} | Track: {current_track} | Server: {current_server}")
                        save_lap(driver.guid, driver.model, current_track, current_server, ac_lap_time, True, now)
                else:
                    print(f"🏁 [LAP] ⚠️ INVALID | {driver.name} | Time: {ac_lap_time/1000:.3f}s | Cuts: {cuts}")

            # ==========================================
            # CLIENT_EVENT (130) - Log only
            # ==========================================
            elif packet_type == ACSP.CLIENT_EVENT:  # 130
                pass  # No timer logic needed, using server lap times

        except Exception as e:
            print(f"❌ Error processing packet: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()


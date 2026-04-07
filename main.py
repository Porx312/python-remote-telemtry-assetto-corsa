import socket
import select
import threading
import time
import os
from dotenv import load_dotenv

from db.database import init_db
from core.config_loader import load_server_configs
from core.session_manager import ServerState, send_registration
from core.packet_processor import process_packet
from network.event_dispatcher import send_server_event

load_dotenv()

SERVER_IP = '127.0.0.1'
GHOST_DRIVER_TIMEOUT_MS = int(os.getenv("GHOST_DRIVER_TIMEOUT_MS", "90000"))

# ──────────────────────────────────────────────
# SERVER LISTENER THREAD
# ──────────────────────────────────────────────

def listen_server(server_state):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_IP, server_state.port))
    sock.setblocking(False)
    server_state.sock = sock

    print(f"🎧 Events listener started on port {server_state.port} — Server: {server_state.server_name}")

    # Force connection immediately so we can get NEW_SESSION and active players
    server_state.last_server_addr = (SERVER_IP, server_state.server_cmd_port)
    send_registration(server_state, SERVER_IP)

    while True:
        ready = select.select([sock], [], [], 0.5)
        if ready[0]:
            try:
                data, addr = sock.recvfrom(4096)
                process_packet(data, server_state, addr)
            except ConnectionResetError:
                # This happen on Windows if a previous sendto() failed (ICMP Port Unreachable).
                # It's safe to ignore for UDP.
                pass
            except Exception as e:
                print(f"❌ [{server_state.port}] Packet error: {e}")

# ──────────────────────────────────────────────
# SERVER STATUS SYNC THREAD
# ──────────────────────────────────────────────

def server_status_loop(servers):
    """
    Sends a "server_status" webhook every 15 seconds
    and polls the server for CAR_INFO to clean up ghosts that dropped while loading.
    """
    import struct
    while True:
        # Sleep first so we don't spam instantly on boot
        time.sleep(15)
        for state in servers.values():
            if not state.last_server_addr:
                continue # Never got a packet from this server yet
            
            # Ping AC server for all slots to detect silent disconnects
            for i in range(32):
                packet = struct.pack('BB', 201, i)
                try:
                    state.sock.sendto(packet, state.last_server_addr)
                except Exception:
                    pass
                time.sleep(0.01)

            # Build list of active players safely (values might change during loop)
            now_ms = int(time.time() * 1000)
            players = []
            stale_car_ids = []
            for car_id, d in list(state.active_drivers.items()):
                last_seen = getattr(d, "last_seen_ms", 0)
                if last_seen and (now_ms - last_seen) > GHOST_DRIVER_TIMEOUT_MS:
                    stale_car_ids.append(car_id)
                    continue
                if not d.guid.startswith('unknown_'):
                    players.append({
                        "steamId": d.guid,
                        "name": d.name,
                        "carModel": d.model
                    })

            # Purga defensiva de "ghost players" cuando no llegaron paquetes de salida.
            for car_id in stale_car_ids:
                d = state.active_drivers.get(car_id)
                if not d:
                    continue
                if d.guid in state.guid_to_driver:
                    del state.guid_to_driver[d.guid]
                del state.active_drivers[car_id]
                if not d.guid.startswith('unknown_'):
                    send_server_event("player_leave", getattr(state, 'config_server_name', state.server_name), {
                        "steamId": d.guid,
                        "trackName": state.track,
                        "trackConfig": state.config
                    })
            if stale_car_ids:
                print(f"🧹 [{state.port}] Purga estado: {len(stale_car_ids)} ghost(s) removidos por timeout")
            
            send_server_event("server_status", getattr(state, 'config_server_name', state.server_name), {
                "players": players,
                "trackName": state.track,
                "trackConfig": state.config
            })

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    init_db()
    
    # Load all server configurations into ServerState objects
    servers = load_server_configs(ServerState)

    if not servers:
        print("❌ No event server configurations found. Check EVENTS_SERVERS_PATH in .env")
        return

    threads = []
    for server_state in servers.values():
        t = threading.Thread(target=listen_server, args=(server_state,), daemon=True)
        t.start()
        threads.append(t)

    # Start 5-minute sync loop in the background
    sync_thread = threading.Thread(target=server_status_loop, args=(servers,), daemon=True)
    sync_thread.start()
    threads.append(sync_thread)

    print(f"\n✅ {len(servers)} event server(s) running. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n👋 Stopping event servers.")

if __name__ == "__main__":
    main()

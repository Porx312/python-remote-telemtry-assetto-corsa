import struct
import threading
import time
from db.database import get_active_battle_config
from engines.battle_engine import BattleManager
from engines.event_engine import TimeAttackEngine

class DriverInfo:
    def __init__(self, name, guid, model):
        self.name = name
        self.guid = guid
        self.model = model
        self.last_seen_ms = int(time.time() * 1000)
        self.lap_count = 0
        self.best_lap = 0
        self.last_lap = 0
        self.car_id = None
        self.lap_start_time = 0
        self.had_collision = False
        self.restarted_lap = False
        self.has_finished = False
        self.last_pos_time = 0
        self.was_idle = False
        self.collision_notified = False
        self.lap_notified_fail = False
        self.idle_notified = False
        self.failed_laps = 0


class ServerState:
    def __init__(self, port, server_cmd_port, track, config, server_name, cfg_path=None):
        self.port = port
        self.server_cmd_port = server_cmd_port
        self.track = track
        self.config = config
        self.config_server_name = server_name
        self.server_name = server_name
        self.cfg_path = cfg_path
        
        self.active_drivers = {} # car_id -> DriverInfo
        self.guid_to_driver = {} # guid -> DriverInfo
        self.sock = None
        self.last_server_addr = None
        
        # Sub-engines
        self.battle_manager = BattleManager()
        self.battle_manager.on_battle_start = self.handle_battle_start
        self.battle_manager.on_score_update = self.handle_battle_score
        self.battle_manager.on_session_restart = self.handle_battle_restart
        self.battle_manager.on_chat_message = self.handle_chat_message

        # Generic Time/Endurance Event logic engine
        self.event_engine = TimeAttackEngine(
            send_chat_callback=lambda car_id, msg: send_chat(self, car_id, msg),
            send_admin_command_callback=lambda cmd: send_admin_command(self, cmd),
            server_state_ref=self
        )

    def _get_battle(self):
        # Nombres desde AC / ini pueden diferir en espacios; Supabase debe coincidir.
        names = [
            (self.server_name or "").strip(),
            (self.config_server_name or "").strip(),
            "server",
        ]
        seen = set()
        for n in names:
            if not n or n in seen:
                continue
            seen.add(n)
            battle = get_active_battle_config(n)
            if battle:
                return battle
        return None

    def handle_battle_start(self, car1_guid, car2_guid):
        config = self._get_battle()
        if config: return config['battle_id']
        return None

    def handle_battle_score(self, battle_id, p1_score, p2_score, winner_guid, points_log):
        from network.event_dispatcher import dispatch_battle_webhook
        config = self._get_battle()
        if config and config['battle_id'] == battle_id:
            dispatch_battle_webhook(self, config, p1_score, p2_score, winner_guid, points_log)

    def handle_battle_restart(self):
        send_admin_command(self, "/restart_session")
        
    def handle_chat_message(self, guid, message):
        driver = self.guid_to_driver.get(guid)
        if driver:
            for c_id, d in self.active_drivers.items():
                if d.guid == guid:
                    send_chat(self, c_id, message)
                    break

def send_registration(server_state, server_ip):
    """Subscribe to the game server to receive telemetry and request initial slot status."""
    sock = server_state.sock
    target = (server_ip, server_state.server_cmd_port)
    print(f"✉️ Registering with {server_ip}:{server_state.server_cmd_port} <- listen on {server_state.port}...")

    # Handshake
    sock.sendto(struct.pack('B', 0), target)
    # Subscribe Update (200) - interval=50ms
    sock.sendto(struct.pack('<BH', 200, 50), target)
    # Get Session Info (59)
    sock.sendto(struct.pack('B', 59), target)

    # Request Car Info for first 32 slots slowly
    def _request():
        for i in range(32):
            packet = struct.pack('BB', 201, i)
            sock.sendto(packet, target)
            time.sleep(0.05)
    
    threading.Thread(target=_request, daemon=True).start()

def send_chat(server_state, car_id, message):
    """Sends a private chat message to a player ID."""
    if not server_state.last_server_addr: return
    try:
        sock = server_state.sock
        target = (server_state.last_server_addr[0], server_state.server_cmd_port)
        
        # Format: [byte id] [byte car_id] [byte len] [wstring mensaje encoded in utf-32le]
        msg_bytes = message.encode('utf-32le')
        msg_len = len(message)
        
        packet = struct.pack(f'<BBB{len(msg_bytes)}s', 202, car_id, msg_len, msg_bytes)
        sock.sendto(packet, target)
    except Exception as e:
        print(f"❌ Error sending chat: {e}")

def send_admin_command(server_state, command):
    """Executes a command natively via ACSP_ADMIN_COMMAND (208) which server plugins intercept."""
    if not server_state.last_server_addr: return
    try:
        sock = server_state.sock
        target = (server_state.last_server_addr[0], server_state.server_cmd_port)
        
        # Format: [byte id] [byte len] [wstring command encoded in utf-32le]
        cmd_bytes = command.encode('utf-32le')
        cmd_len = len(command)
        
        packet = struct.pack(f'<BB{len(cmd_bytes)}s', 208, cmd_len, cmd_bytes)
        sock.sendto(packet, target)
    except Exception as e:
        print(f"❌ Error sending admin command: {e}")

import struct
import threading
import time
import os
import os.path
from uuid import uuid4
from db.database import get_server_mode_for_instance
from engines.battlesystem.orchestrator import BattleManager
from engines.event_engine import TimeAttackEngine

def _parse_targeted_pit_commands():
    """
    Comandos por jugador para enviar a pits/spectator.
    Usa placeholders: {car_id}, {steam_id}, {name}
    Ej: "/pit {car_id};/spec {car_id}"
    """
    raw = (os.getenv("BATTLE_TARGETED_PIT_COMMANDS", "") or "").strip()
    if not raw:
        return ["/pit {car_id}"]
    parts = [p.strip() for p in raw.replace(",", ";").split(";")]
    return [p for p in parts if p]

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
        self.server_folder_id = ""
        if self.cfg_path:
            try:
                # cfg_path -> .../<server-folder>/cfg/server_cfg.ini => take parent of "cfg".
                cfg_dir = os.path.dirname(self.cfg_path)
                server_dir = os.path.dirname(cfg_dir)
                self.server_folder_id = os.path.basename(server_dir).strip()
            except Exception:
                self.server_folder_id = ""
        
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

    def _get_server_mode(self):
        # Nombres desde AC / ini pueden diferir en espacios; panel/control debe coincidir.
        names = [
            (self.server_folder_id or "").strip(),
            (self.server_name or "").strip(),
            (self.config_server_name or "").strip(),
        ]
        seen = set()
        for n in names:
            if not n or n in seen:
                continue
            seen.add(n)
            mode = get_server_mode_for_instance(n)
            if mode:
                return mode
        return ""

    def _get_battle_webhook_url(self):
        # Battle must use dedicated webhook endpoint only.
        return (
            (os.getenv("BATTLE_WEBHOOK_URL") or "").strip()
            or (os.getenv("server_battle_webhook_url") or "").strip()
        )

    def handle_battle_start(self, car1_guid, car2_guid):
        if self._get_server_mode() != "battle":
            return None
        return f"battle-{uuid4().hex[:12]}"

    def handle_battle_score(
        self,
        battle_id,
        p1_score,
        p2_score,
        winner_guid,
        points_log,
        car1_guid=None,
        car2_guid=None,
    ):
        from network.event_dispatcher import dispatch_battle_webhook
        webhook_url = self._get_battle_webhook_url()
        # Only dispatch final results (series winner decided).
        if not winner_guid:
            return
        if self._get_server_mode() != "battle" or not battle_id or not webhook_url:
            return

        p1_guid = car1_guid
        p2_guid = car2_guid
        if not p1_guid or not p2_guid:
            print("⚠️ [BATTLE] score update missing pair GUIDs; webhook skipped")
            return
        p1_driver = self.guid_to_driver.get(p1_guid)
        p2_driver = self.guid_to_driver.get(p2_guid)

        battle_config = {
            "battle_id": battle_id,
            "player1_steam_id": p1_guid,
            "player2_steam_id": p2_guid,
            "webhook_url": webhook_url,
            "webhook_secret": (
                (os.getenv("BATTLE_WEBHOOK_SECRET") or "").strip()
                or (os.getenv("battle_webhook_secret") or "").strip()
                or None
            ),
            "metadata": {
                "player1Name": getattr(p1_driver, "name", ""),
                "player2Name": getattr(p2_driver, "name", ""),
                "player1Car": getattr(p1_driver, "model", ""),
                "player2Car": getattr(p2_driver, "model", ""),
                "track": self.track,
                "trackConfig": self.config,
            },
        }
        dispatch_battle_webhook(self, battle_config, p1_score, p2_score, winner_guid, points_log)

    def handle_battle_restart(self, car1_guid=None, car2_guid=None):
        use_global_restart = (
            (os.getenv("BATTLE_USE_GLOBAL_RESTART", "false").strip().lower())
            in ("1", "true", "yes", "on")
        )
        use_targeted_pit = (
            (os.getenv("BATTLE_USE_TARGETED_PIT", "true").strip().lower())
            in ("1", "true", "yes", "on")
        )
        targeted_pit_only_when_two = (
            (os.getenv("BATTLE_TARGETED_PIT_ONLY_WHEN_SERVER_HAS_2", "true").strip().lower())
            in ("1", "true", "yes", "on")
        )

        if use_global_restart:
            send_admin_command(self, "/restart_session")
            return

        if not use_targeted_pit:
            return

        connected_real_drivers = sum(
            1 for d in self.active_drivers.values()
            if d.guid and not str(d.guid).startswith("unknown_")
        )
        if targeted_pit_only_when_two and connected_real_drivers > 2:
            print(
                "⚠️ [BATTLE] Targeted pit skipped in open server "
                f"(connected drivers: {connected_real_drivers})."
            )
            return

        command_templates = _parse_targeted_pit_commands()
        cmd_delay_ms = int((os.getenv("BATTLE_TARGETED_PIT_DELAY_MS", "120") or "120").strip() or "120")
        cmd_delay_sec = max(0, cmd_delay_ms) / 1000.0

        # Open-server mode: send only this battle pair to pits/spectator.
        targets = [car1_guid, car2_guid]
        sent = 0
        for guid in targets:
            if not guid:
                continue
            for c_id, d in self.active_drivers.items():
                if d.guid == guid:
                    for template in command_templates:
                        try:
                            cmd = template.format(
                                car_id=c_id,
                                steam_id=d.guid,
                                name=(d.name or "").strip(),
                            )
                        except Exception:
                            continue
                        if not cmd:
                            continue
                        send_admin_command(self, cmd)
                        print(f"🎯 [BATTLE] targeted cmd for {d.name} ({d.guid}): {cmd}")
                        if cmd_delay_sec > 0:
                            time.sleep(cmd_delay_sec)
                    sent += 1
                    break
        if sent:
            print(f"🎯 [BATTLE] Sent targeted pit/spectator cmds to {sent} battle player(s)")
        
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

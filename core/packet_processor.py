import time
import os
import re
from network.ac_packet import ACSP, PacketParser
from core.session_manager import DriverInfo, send_registration, send_chat, send_admin_command
from db.database import save_driver, save_lap, get_active_server_event
from network.event_dispatcher import dispatch_event, dispatch_battle_webhook, send_server_event

MIN_VALID_LAP_MS = int(os.getenv("MIN_VALID_LAP_MS", "10000"))
GHOST_DRIVER_TIMEOUT_MS = int(os.getenv("GHOST_DRIVER_TIMEOUT_MS", "90000"))


def _mark_driver_seen(driver):
    driver.last_seen_ms = int(time.time() * 1000)


def _battle_guids(battle_cfg):
    p1 = (battle_cfg.get("player1_steam_id") or "").strip()
    p2 = (battle_cfg.get("player2_steam_id") or "").strip()
    return p1, p2


def _is_battle_player(guid, battle_cfg):
    if not guid or not battle_cfg:
        return False
    g = guid.strip()
    p1, p2 = _battle_guids(battle_cfg)
    return g == p1 or g == p2


def _drop_stale_drivers_on_new_session(server_state, now_ms):
    """
    Tras reinicios/rotaciones del server pueden quedar drivers "fantasma" en memoria
    si no llegó CONNECTION_CLOSED. Solo quita entradas con last_seen antiguo (no borrar
    a todos los que last_seen==0: eso vaciaba el lobby y rompía batallas).
    """
    removed = 0
    for car_id, driver in list(server_state.active_drivers.items()):
        last_seen = getattr(driver, "last_seen_ms", 0)
        if last_seen <= 0:
            continue
        if (now_ms - last_seen) <= GHOST_DRIVER_TIMEOUT_MS:
            continue
        if driver.guid in server_state.guid_to_driver:
            del server_state.guid_to_driver[driver.guid]
        server_state.battle_manager.remove_car(driver.guid)
        del server_state.active_drivers[car_id]
        removed += 1
    if removed:
        print(f"🧹 [{server_state.port}] Limpieza NEW_SESSION: {removed} ghost(s) removidos")


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
        now_ms = int(time.time() * 1000)
        _drop_stale_drivers_on_new_session(server_state, now_ms)

        parser.read_uint8()   # version
        parser.read_uint8()   # sessionIndex
        parser.read_uint8()   # currentSessionIndex
        parser.read_uint8()   # sessionCount

        # NEW_SESSION is a mixed bag: 
        # - Server Name is typically a wstring (UTF-32)
        # - Track and Config are typically standard strings (1 byte length)
        server_state.server_name = parser.read_wstring()
        server_state.track       = parser.read_string()
        server_state.config      = parser.read_string()

        # If we have a cfg_path, reload it to update config_server_name and ensure track info
        if server_state.cfg_path and os.path.exists(server_state.cfg_path):
            try:
                with open(server_state.cfg_path, 'rb') as f:
                    raw = f.read()
                try:
                    content = raw.decode('utf-8')
                except UnicodeDecodeError:
                    content = raw.decode('utf-16le', errors='ignore')

                # Update server names
                server_name_m = re.search(r'^SERVER_NAME=(.+)', content, re.MULTILINE)
                if not server_name_m:
                    server_name_m = re.search(r'^NAME=(.+)', content, re.MULTILINE)
                if server_name_m:
                    server_state.config_server_name = server_name_m.group(1).strip()

                # Robustly update track/config from INI if packet data looks weird or we want disk priority
                track_m  = re.search(r'^TRACK=(.+)', content, re.MULTILINE)
                config_m = re.search(r'^CONFIG_TRACK=(.*)', content, re.MULTILINE)
                if track_m:
                    server_state.track = track_m.group(1).strip()
                if config_m:
                    server_state.config = config_m.group(1).strip()
                
                print(f"🔄 [{server_state.port}] Config reloaded from {server_state.cfg_path}")
            except Exception as e:
                print(f"❌ Error reloading {server_state.cfg_path}: {e}")

        # Log active event for this server (try session name, then config name)
        print(f"   🔍 DB Lookup: '{server_state.server_name}' or '{server_state.config_server_name}'")
        event = get_active_server_event(server_state.server_name)
        if not event:
            event = get_active_server_event(server_state.config_server_name)
        
        battle = server_state._get_battle()
            
        if event:
            event_info = f" | 🎮 Event: {event['event_type']}"
        elif battle:
            event_info = f" | ⚔️  Battle: {battle['player1_steam_id']} vs {battle['player2_steam_id']}"
        else:
            event_info = " | ⚠️  No Event/Battle registered"

        print(f"🌍 Session [{server_state.port}]: {server_state.track} ({server_state.config}) | Name: {server_state.server_name}{event_info}")

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
        _mark_driver_seen(driver)
        server_state.active_drivers[car_id] = driver
        if guid and not guid.startswith('unknown_'):
            server_state.guid_to_driver[guid] = driver

        print(f"🟢 [{server_state.port}] [CONNECTED] CarID {car_id} | {name} | {model} | {guid}")
        server_state.battle_manager.set_driver_name(guid, name)
        save_driver(guid, name, model)

        driver.lap_start_time = time.time() * 1000
        driver.lap_notified_fail = False

        # Notify Node.js the player joined (Event webhook dropped as it's not a lap update)
        if not guid.startswith('unknown_'):
            
            # Send battle webhook on connect so frontend knows player is alive
            battle_cfg = server_state._get_battle()
            if battle_cfg and _is_battle_player(guid, battle_cfg):
                if server_state.battle_manager.battle:
                    p1_score = server_state.battle_manager.battle.car1_score
                    p2_score = server_state.battle_manager.battle.car2_score
                    points_log = server_state.battle_manager.battle.points_log
                else:
                    p1_score, p2_score, points_log = 0, 0, []
                dispatch_battle_webhook(server_state, battle_cfg, p1_score, p2_score, None, points_log)
            elif battle_cfg:
                p1, p2 = _battle_guids(battle_cfg)
                print(
                    f"⚠️ [{server_state.port}] [BATTLE] GUID no coincide para {name}: "
                    f"'{guid.strip()}' vs esperados '{p1}' / '{p2}'"
                )

            # Node.js General Webhook
            send_server_event("player_join", server_state.server_name, {
                "steamId": guid,
                "name": name,
                "carModel": model,
                "trackName": server_state.track,
                "trackConfig": server_state.config
            })

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

        # If AC says this slot is empty OR the player aborted load (connected but no name/guid),
        # but we still have them tracked as an active driver...
        if is_connected == 0 or not name or not guid:
            driver = server_state.active_drivers.get(car_id)
            if driver:
                print(f"🧹 [{server_state.port}] Cleaning up Ghost Player: {driver.name} (CarID {car_id})")
                
                # Despawn Battle logic
                battle_cfg = server_state._get_battle()
                if battle_cfg and _is_battle_player(driver.guid, battle_cfg):
                    if server_state.battle_manager.battle:
                        p1_score = server_state.battle_manager.battle.car1_score
                        p2_score = server_state.battle_manager.battle.car2_score
                        points_log = server_state.battle_manager.battle.points_log
                    else:
                        p1_score, p2_score, points_log = 0, 0, []
                    dispatch_battle_webhook(server_state, battle_cfg, p1_score, p2_score, None, points_log)
                    
                # Node.js Event Leave
                if not driver.guid.startswith('unknown_'):
                    send_server_event("player_leave", getattr(server_state, 'config_server_name', server_state.server_name), {
                        "steamId": driver.guid,
                        "trackName": server_state.track,
                        "trackConfig": server_state.config
                    })

                server_state.battle_manager.remove_car(driver.guid)
                if driver.guid in server_state.guid_to_driver:
                    del server_state.guid_to_driver[driver.guid]
                del server_state.active_drivers[car_id]
            return

        if not name or not guid: return

        # DO NOT wipe existing driver state (laps, penalties) on heartbeat ping
        driver = server_state.active_drivers.get(car_id)
        if not driver:
            driver = DriverInfo(name, guid, model)
            _mark_driver_seen(driver)
            server_state.active_drivers[car_id] = driver
        else:
            driver.name = name
            driver.guid = guid
            driver.model = model
            _mark_driver_seen(driver)
            
        if guid and not guid.startswith('unknown_'):
            server_state.guid_to_driver[guid] = driver

        print(f"🏎️ [{server_state.port}] [CAR_INFO] CarID {car_id} | {name} | {model} | {guid}")
        server_state.battle_manager.set_driver_name(guid, name)
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
            if not driver.guid.startswith('unknown_'):
                dispatch_event(server_state, driver, driver.last_lap, is_finished=True)
                
                battle_cfg = server_state._get_battle()
                if battle_cfg and _is_battle_player(driver.guid, battle_cfg):
                    if server_state.battle_manager.battle:
                        p1_score = server_state.battle_manager.battle.car1_score
                        p2_score = server_state.battle_manager.battle.car2_score
                        points_log = server_state.battle_manager.battle.points_log
                    else:
                        p1_score, p2_score, points_log = 0, 0, []
                    dispatch_battle_webhook(server_state, battle_cfg, p1_score, p2_score, None, points_log)
                    
                # Node.js General Webhook
                send_server_event("player_leave", server_state.server_name, {
                    "steamId": driver.guid,
                    "trackName": server_state.track,
                    "trackConfig": server_state.config
                })

                server_state.battle_manager.remove_car(driver.guid)
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
            _mark_driver_seen(driver)
            speed_ms = ((v_x or 0)**2 + (v_y or 0)**2 + (v_z or 0)**2)**0.5
            now = int(time.time() * 1000)
            
            # Feed Time Attack/Endurance engine
            event = get_active_server_event(server_state.server_name) or get_active_server_event(server_state.config_server_name)
            meta = event.get("metadata", {}) if event else {}
            
            driver.car_id = car_id
            server_state.event_engine.check_idle(driver, speed_ms, now, meta)

            # Feed BattleManager if active
            battle_cfg = server_state._get_battle()
            if battle_cfg and _is_battle_player(driver.guid, battle_cfg):
                server_state.battle_manager.update(
                    driver.guid, spline, speed_ms * 3.6, (pos_x, pos_y, pos_z)
                )

    # ─── CLIENT_EVENT (130) ─────────────────────────────────
    elif packet_type == getattr(ACSP, 'CLIENT_EVENT', 130):
        ev_type = parser.read_uint8()
        car_id  = parser.read_uint8()
        
        # Battle Engine Collision Check
        if ev_type == getattr(ACSP, 'CE_COLLISION_WITH_CAR', 10):
            other_car_id = parser.read_uint8()
            impact_speed = parser.read_float()
            driver1 = server_state.active_drivers.get(car_id)
            driver2 = server_state.active_drivers.get(other_car_id)
            battle_cfg = server_state._get_battle()
            if battle_cfg and driver1 and driver2:
                if _is_battle_player(driver1.guid, battle_cfg) and _is_battle_player(
                    driver2.guid, battle_cfg
                ):
                    server_state.battle_manager.handle_collision(
                        driver1.guid, driver2.guid, impact_speed
                    )
        elif ev_type == getattr(ACSP, 'CE_COLLISION_WITH_ENV', 11):
            pass # We don't track ENV collisions for battles yet
        
        if ev_type in (getattr(ACSP, 'CE_COLLISION_WITH_CAR', 10), getattr(ACSP, 'CE_COLLISION_WITH_ENV', 11)):
            driver = server_state.active_drivers.get(car_id)
            if driver:
                driver.car_id = car_id
                event = get_active_server_event(server_state.server_name) or get_active_server_event(server_state.config_server_name)
                meta = event.get("metadata", {}) if event else {}
                server_state.event_engine.check_collision(driver, meta)

    # ─── LAP_COMPLETED (58) ─────────────────────────────────
    elif packet_type == ACSP.LAP_COMPLETED:
        car_id      = parser.read_uint8()
        if car_id is None: return
        ac_lap_time = parser.read_uint32() or 0
        cuts        = parser.read_uint8() or 0

        now    = int(time.time() * 1000)
        driver = server_state.active_drivers.get(car_id)

        if not driver:
            import struct
            driver = DriverInfo(f"Driver_CarID_{car_id}", f"unknown_{car_id}", "Unknown")
            _mark_driver_seen(driver)
            server_state.active_drivers[car_id] = driver
            if server_state.last_server_addr:
                server_state.sock.sendto(struct.pack('BB', 201, car_id), server_state.last_server_addr)
        else:
            _mark_driver_seen(driver)

        if ac_lap_time <= 0 or ac_lap_time > 36000000:
            return

        if ac_lap_time < MIN_VALID_LAP_MS:
            print(
                f"⚠️ [{server_state.port}] Lap ignorada por sospechosa ({ac_lap_time/1000:.3f}s < {MIN_VALID_LAP_MS/1000:.3f}s)"
            )
            return

        driver.last_lap   = ac_lap_time
        driver.lap_count += 1
        is_valid = (cuts == 0)

        # Get active event settings to check constraints
        event = get_active_server_event(server_state.server_name)
        if not event:
            event = get_active_server_event(server_state.config_server_name)

        meta = event.get("metadata", {}) if event else {}
        total_laps = meta.get("totalLaps", "?")
        
        driver.car_id = car_id
        is_valid, fail_reason = server_state.event_engine.evaluate_lap(driver, ac_lap_time, cuts, meta)

        if not is_valid:
            print(f"🏁 [{server_state.port}] [LAP] ⚠️  INVALID | {driver.name} | {ac_lap_time/1000:.3f}s | Cuts: {cuts} ({fail_reason})")
            
            # Send webhook to update failed lap counts in real time
            dispatch_event(server_state, driver, lap_time_ms=0, is_finished=False)
            return

        if driver.best_lap == 0 or ac_lap_time < driver.best_lap:
            driver.best_lap = ac_lap_time

        print(f"🏁 [{server_state.port}] [LAP] ✅ | {driver.name} | Lap #{driver.lap_count} | {ac_lap_time/1000:.3f}s | Best: {driver.best_lap/1000:.3f}s")
        if event:
            send_chat(server_state, car_id, f"[EVENT] Lap {driver.lap_count}/{total_laps} COMPLETED! Time: {ac_lap_time/1000:.3f}s")

        if not driver.guid.startswith('unknown_'):
            save_lap(driver.guid, driver.model, server_state.track, server_state.config,
                     server_state.server_name, ac_lap_time, True, now)
            
            # Node.js General Webhook
            send_server_event("lap_completed", server_state.server_name, {
                "steamId": driver.guid,
                "carModel": driver.model,
                "trackName": server_state.track,
                "trackConfig": server_state.config,
                "lapTime": ac_lap_time
            })

        # ── Dispatch dynamic webhook based on active event ──
        dispatch_event(server_state, driver, lap_time_ms=ac_lap_time)

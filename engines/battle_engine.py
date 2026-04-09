import math
import os
import threading
import time

# ===================================================
# TEST MODE: True = allows 1 player for solo testing.
# Set to False in production to require 2 players.
# ===================================================
TEST_MODE_1_PLAYER = False

# Points needed to win the series (Best of N)
POINTS_TO_WIN = 2

COLLISION_MIN_IMPACT = float(os.getenv("COLLISION_MIN_IMPACT", "3.0"))
COLLISION_MIN_REL_SPEED = float(os.getenv("COLLISION_MIN_REL_SPEED", "8.0"))
BRAKE_CHECK_DELTA_KMH = float(os.getenv("BRAKE_CHECK_DELTA_KMH", "8.0"))
MAX_BATTLE_GAP_METERS = float(os.getenv("MAX_BATTLE_GAP_METERS", "45.0"))
# If distance grows too much during ACTIVE, award lead by outrun immediately.
OUTRUN_GAP_AUTO_POINT_METERS = float(os.getenv("OUTRUN_GAP_AUTO_POINT_METERS", "150.0"))
# Seconds after showing point messages before /restart_session (pits)
POINT_TO_PITS_DELAY_SEC = float(os.getenv("POINT_TO_PITS_DELAY_SEC", "3.0"))
# After /restart_session, ignore gap-based aborts (avoids double restart at 0 km/h / bad positions)
POST_RESTART_GAP_GRACE_SEC = float(os.getenv("BATTLE_POST_RESTART_GAP_GRACE_SEC", "15.0"))
# After requesting /restart_session, pause battle logic briefly to avoid duplicate restarts.
RESTART_SETTLE_SEC = float(os.getenv("BATTLE_RESTART_SETTLE_SEC", "8.0"))


class CarState:
    """Tracks the real-time and accumulated state of a single driver."""
    def __init__(self, guid):
        self.guid = guid
        self.spline = 0.0
        self.speed = 0.0
        self.pos = (0.0, 0.0, 0.0)
        # Accumulated spline distance driven (handles 0→1 wrap correctly)
        self.driven_spline = 0.0
        self.last_update_time = 0.0

    def update(self, spline, speed, pos):
        now = time.time()
        if self.last_update_time > 0:
            delta = (spline - self.spline) % 1.0
            if delta > 0.5:
                delta -= 1.0
            elif delta < -0.5:
                delta += 1.0
            if delta > 0:
                self.driven_spline += delta
        self.spline = spline
        self.speed = speed
        self.pos = pos
        self.last_update_time = now


class TougeBattle:
    """Holds the data for a 1v1 Cat-and-Mouse Touge series."""
    def __init__(self, car1_guid, car2_guid):
        self.car1_guid = car1_guid
        self.car2_guid = car2_guid
        self.car1_score = 0
        self.car2_score = 0
        self.run_count = 0
        self.lead_guid = None
        self.chase_guid = None
        self.initial_gap_spline = 0.0
        self.winner = None
        # Each element: {'scorer': guid, 'reason': str, 'ts': unix_ms}
        self.points_log = []

    def get_opponent(self, guid):
        return self.car2_guid if guid == self.car1_guid else self.car1_guid


class BattleManager:
    """
    Manages the full Touge battle state machine:
    IDLE -> ARMED -> LAUNCHING -> ACTIVE -> FINISHED
    """
    def __init__(self):
        self.state = "IDLE"
        self.cars = {}      # guid -> CarState
        self.battle = None  # TougeBattle instance

        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0

        # Callbacks set by main.py to        # Callbacks to emit events to main.py
        self.on_battle_start = None      # kwargs: (car1_guid, car2_guid) -> returns battle_id
        self.on_score_update = None      # kwargs: (battle_id, p1_score, p2_score, winner_guid, log)
        self.on_session_restart = None   # kwargs: () -> calls /restart_session
        self.on_chat_message = None      # kwargs: (guid, msg) -> sends /chat to specific user
        
        # State
        self.battle = None

        self.battle_id = None  # DB row id of the current active battle

        # Auto-reset after FINISHED
        self.finished_time = 0.0
        self.FINISHED_COOLDOWN = 10.0  # Seconds before accepting a new battle

        # Battle config
        self.run_length_spline = 0.80   # Virtual finish line at 80% of spline
        self.judge_offset_spline = 0.03 # Draw tolerance
        self.overtake_margin_spline = 0.003 # Chase needs to be visibly ahead to pass
        self.active_start_time = 0.0
        self._restart_timer = None
        # guid -> display name (from telemetry); used in scoreboard chat lines
        self.player_names = {}
        # After session restart, skip prestart/launch gap aborts until this time (unix)
        self._gap_abort_suppressed_until = 0.0
        # While waiting for AC to fully apply /restart_session, freeze state transitions.
        self._restart_settle_until = 0.0

    def set_driver_name(self, guid, name):
        if not guid or not name or str(guid).startswith("unknown"):
            return
        self.player_names[guid] = str(name).strip()

    def _display_name(self, guid):
        if not guid:
            return "?"
        n = self.player_names.get(guid)
        if n:
            return n
        return f"...{guid[-6:]}" if len(guid) > 6 else guid

    def _scoreboard_line(self):
        g1, g2 = self.battle.car1_guid, self.battle.car2_guid
        return (
            f"{self._display_name(g1)} {self.battle.car1_score} : "
            f"{self._display_name(g2)} {self.battle.car2_score}"
        )

    def _pit_suffix(self):
        """Short hint: next spawn pits (no countdown)."""
        return " → pits"

    def _format_point_broadcast(self, winner_guid, reason):
        board = self._scoreboard_line()
        pit = self._pit_suffix()
        if reason == "draw":
            return f"[TOUGE] DRAW | {board}{pit}"
        if reason == "overtake":
            return f"[TOUGE] OVERTAKE | {board}{pit}"
        if reason == "outrun":
            return f"[TOUGE] OUTRUN | {board}{pit}"
        if reason == "outrun_gap":
            return f"[TOUGE] OUTRUN GAP | {board}{pit}"
        if reason == "dnf_lead_stalled":
            return f"[TOUGE] DNF lead | {board}{pit}"
        if reason == "dnf_chase_stalled":
            return f"[TOUGE] DNF chase | {board}{pit}"
        if reason == "collision_penalty":
            return f"[TOUGE] HIT rear | {board}{pit}"
        if reason == "collision_brake_check":
            return f"[TOUGE] HIT brake | {board}{pit}"
        return f"[TOUGE] PT {reason} | {board}{pit}"

    def _cancel_restart_timer(self):
        if self._restart_timer is not None:
            self._restart_timer.cancel()
            self._restart_timer = None

    def _send_chat_sequence(self, items):
        """Send in order: str → both; (guid, str) → one driver."""
        if not self.on_chat_message or not items:
            return
        for item in items:
            if isinstance(item, tuple) and len(item) == 2:
                guid, msg = item
                self.on_chat_message(guid, msg)
            else:
                self.on_chat_message(self.battle.car1_guid, item)
                self.on_chat_message(self.battle.car2_guid, item)

    def get_distance(self, pos1, pos2):
        return math.sqrt((pos1[0]-pos2[0])**2 + (pos1[1]-pos2[1])**2 + (pos1[2]-pos2[2])**2)

    def update(self, driver_guid, spline, speed, world_position):
        """Called on every CAR_UPDATE packet (packet 53) from the server."""
        if driver_guid not in self.cars:
            self.cars[driver_guid] = CarState(driver_guid)
        self.cars[driver_guid].update(spline, speed, world_position)
        self._process_logic()

    def remove_car(self, driver_guid):
        """Called when a player disconnects."""
        if driver_guid in self.cars:
            del self.cars[driver_guid]
        if self.state in ["ARMED", "LAUNCHING", "ACTIVE", "WAITING_RESTART"]:
            print(f"[BATTLE] Player {driver_guid} disconnected. Cancelling battle.")
            self._reset_to_idle()

    def _reset_to_idle(self, full_reset=False):
        self._cancel_restart_timer()
        self.state = "IDLE"
        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0
        self.finished_time = 0.0
        if full_reset:
            self.battle_id = None

    def handle_collision(self, car1_guid, car2_guid, impact_speed):
        """Called by main.py on CE_COLLISION_WITH_CAR."""
        if self.state != "ACTIVE":
            return
        # In 1-player test mirroring mode, collisions are meaningless.
        if not self.battle or self.battle.car1_guid == self.battle.car2_guid:
            return
            
        def _notify_both(msg):
            if self.on_chat_message:
                self.on_chat_message(self.battle.car1_guid, msg)
                self.on_chat_message(self.battle.car2_guid, msg)

        guids = {car1_guid, car2_guid}
        if self.battle.car1_guid not in guids or self.battle.car2_guid not in guids:
            print(
                f"⚠️ [BATTLE] Collision ignored: participants mismatch "
                f"({car1_guid}, {car2_guid}) vs expected "
                f"({self.battle.car1_guid}, {self.battle.car2_guid})"
            )
            return
            
        lead_car  = self.cars[self.battle.lead_guid]
        chase_car = self.cars[self.battle.chase_guid]
        
        # 1) Ignore true light rubs only.
        #    We consider a contact meaningful if either:
        #    - impact is above threshold, or
        #    - there is a clear speed delta (rear-end / brake check situations).
        relative_speed = abs(chase_car.speed - lead_car.speed)
        if impact_speed < COLLISION_MIN_IMPACT and relative_speed < COLLISION_MIN_REL_SPEED:
            print(
                f"⚠️ [BATTLE] Light rub ignored. Impact: {impact_speed:.2f}, "
                f"Δspeed: {relative_speed:.1f} (Lead: {lead_car.speed:.1f}, Chase: {chase_car.speed:.1f})"
            )
            _notify_both(f"[TOUGE] rub OK ({impact_speed:.0f})")
            return
        
        # 2. Brake Checking Detection
        # If the LEAD car's speed is dangerously slow during the ACTIVE race (e.g. less than 40 km/h) 
        # or they are going significantly slower than the CHASE car (e.g. 15 km/h delta) when the crash happens,
        # we rule it a brake-check / blocking penalty against LEAD.
        if lead_car.speed < 40.0 or (chase_car.speed - lead_car.speed) >= BRAKE_CHECK_DELTA_KMH:
            print(f"💥 [BATTLE] BRAKE CHECK PENALTY! Lead caused crash. Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f} km/h, Chase: {chase_car.speed:.1f} km/h)")
            self._award_point(self.battle.chase_guid, reason='collision_brake_check')
        else:
            # Standard rear-end collision, CHASE is at fault for not maintaining distance.
            print(f"💥 [BATTLE] COLLISION Penalty! Chase hit Lead. Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f} km/h, Chase: {chase_car.speed:.1f} km/h)")
            self._award_point(self.battle.lead_guid, reason='collision_penalty')

    def _process_logic(self):
        now = time.time()

        if self.state == "RESTARTING":
            if now < self._restart_settle_until:
                return
            # Restart settle done; allow engine to arm again.
            self.state = "IDLE"

        # Only consider cars that have sent telemetry in the last 5 seconds
        active_guids = [g for g, c in self.cars.items() if (now - c.last_update_time) < 5.0]

        min_players = 1 if TEST_MODE_1_PLAYER else 2

        if len(active_guids) < min_players:
            # WAITING_RESTART: telemetry often pauses during session swap; do not cancel the pit timer.
            if self.state not in ["IDLE", "FINISHED", "WAITING_RESTART"]:
                print(f"\n[BATTLE] Not enough players ({len(active_guids)}). Resetting.")
                self._reset_to_idle(full_reset=True)
            return

        guids_sorted = sorted(active_guids)
        c1_guid = guids_sorted[0]
        # In test mode with 1 player, mirror car2 = car1 (distance will always be 0)
        c2_guid = guids_sorted[1] if len(guids_sorted) >= 2 else guids_sorted[0]

        # Reset battle if the pair of players changed
        if not self.battle or set([self.battle.car1_guid, self.battle.car2_guid]) != set([c1_guid, c2_guid]):
            self.battle = TougeBattle(c1_guid, c2_guid)
            self._reset_to_idle(full_reset=True)

        if self.state == "WAITING_RESTART":
            return

        if self.state == "FINISHED":
            # Auto-reset after cooldown so a new battle can begin
            if self.finished_time == 0.0:
                self.finished_time = now
            elif now - self.finished_time >= self.FINISHED_COOLDOWN:
                car1 = self.cars[self.battle.car1_guid]
                car2 = self.cars[self.battle.car2_guid]
                # Enforce that drivers must slow down (< 20kmh) or return to the start (< 0.1 spline)
                # before the battle engine will arm a new round, preventing accidental high-speed false starts.
                if (car1.speed < 20.0 and car2.speed < 20.0) or (car1.spline < 0.1 and car2.spline < 0.1) or TEST_MODE_1_PLAYER:
                    print(f"[BATTLE] Cooldown over & Drivers ready. Ready for a new battle!")
                    # Reset battle state so next LAUNCH starts run 1 and generates a new DB row
                    if self.battle:
                        self.battle = TougeBattle(self.battle.car1_guid, self.battle.car2_guid)
                    self._reset_to_idle(full_reset=True)
            return

        car1 = self.cars[self.battle.car1_guid]
        car2 = self.cars[self.battle.car2_guid]
        distance = self.get_distance(car1.pos, car2.pos)

        # ==========================
        # IDLE: Ready for Rolling Start
        # ==========================
        if self.state == "IDLE":
            # Both cars within 40m
            close_enough = distance < 40.0 or TEST_MODE_1_PLAYER
            if close_enough:
                # If they are cruising together, wait for someone to gun it
                if car1.speed >= 25.0 or car2.speed >= 25.0:
                    self.state = "ARMED"
                    print(f"⚡ [BATTLE] ARMED between {car1.guid} and {car2.guid}!")
                    if self.on_chat_message:
                        msg = "[TOUGE] ARMED — ~40 side by side"
                        self.on_chat_message(car1.guid, msg)
                        self.on_chat_message(car2.guid, msg)

        # ==========================
        # ARMED: Waiting for both cars to hit 40 km/h
        # ==========================
        elif self.state == "ARMED":
            # Still loading / 0 km/h in pits: positions can look "far apart" — don't abort yet
            if (
                distance > MAX_BATTLE_GAP_METERS
                and not TEST_MODE_1_PLAYER
                and time.time() >= self._gap_abort_suppressed_until
                and (car1.speed >= 12.0 or car2.speed >= 12.0)
            ):
                self._abort_run_no_point(
                    f"prestart_gap_{distance:.1f}m",
                    [f"[TOUGE] GAP pre ({distance:.0f}m) no PT{self._pit_suffix()}"],
                )
                return

            # Persist battle start to DB physically as soon as rolling start begins
            if self.battle_id is None and self.on_battle_start:
                self.battle_id = self.on_battle_start(
                    self.battle.car1_guid, self.battle.car2_guid
                )

            if car1.speed > 40.0 and car2.speed > 40.0:
                self.state = "LAUNCHING"
                self.launch_trigger_time = now
                print(f"\n[BATTLE] ROLLING START DETECTED! Gap: {distance:.1f}m. Waiting for both cars to hit 40 km/h...")
                if self.on_chat_message:
                    msg = "[TOUGE] GO — 40+"
                    self.on_chat_message(car1.guid, msg)
                    self.on_chat_message(car2.guid, msg)
            elif now - self.launch_trigger_time > 3.0 and self.launch_trigger_time != 0.0: # Only timeout if launch_trigger_time was set
                print("[BATTLE] Timeout: opponent did not reach 40 km/h within 3s. Cancelling.")
                if self.on_chat_message:
                    msg = "[TOUGE] T-out launch"
                    self.on_chat_message(car1.guid, msg)
                    self.on_chat_message(car2.guid, msg)
                self._reset_to_idle()

        # ==========================
        # LAUNCHING: Confirm both cars launch
        # ==========================
        elif self.state == "LAUNCHING":
            if (
                distance > MAX_BATTLE_GAP_METERS
                and not TEST_MODE_1_PLAYER
                and time.time() >= self._gap_abort_suppressed_until
                and (car1.speed >= 12.0 or car2.speed >= 12.0)
            ):
                self._abort_run_no_point(
                    f"launch_gap_{distance:.1f}m",
                    [f"[TOUGE] GAP launch ({distance:.0f}m) no PT{self._pit_suffix()}"],
                )
                return

            if car1.speed > 40.0 and car2.speed > 40.0:
                # Before starting ACTIVE, check for false start (Jump Start)
                # In runs > 1, roles are predetermined. If Chase jumped and passed Lead, penalty!
                if self.battle.run_count >= 1:
                    expected_lead = self.battle.chase_guid
                    expected_chase = self.battle.lead_guid
                    c_lead = self.cars[expected_lead]
                    c_chase = self.cars[expected_chase]
                    jump_gap = (c_chase.spline - c_lead.spline) % 1.0
                    # If jump_gap < 0.5, chase is ahead of lead -> False Start
                    if jump_gap < 0.5 and jump_gap > 0.001:  # Added a small margin for side-by-side
                        nl = self._display_name(expected_lead)
                        nc = self._display_name(expected_chase)
                        order_line = f"L {nl} / C {nc}"
                        print(
                            f"🚨 [BATTLE] FALSE START | want {order_line} | "
                            f"chase ahead of lead"
                        )
                        self._abort_run_no_point(
                            "false_start",
                            [
                                (expected_chase, f"[TOUGE] FS CHASE | ok: {order_line}{self._pit_suffix()}"),
                                (expected_lead, f"[TOUGE] FS | ok: {order_line}{self._pit_suffix()}"),
                                f"[TOUGE] FS order | {order_line} no PT{self._pit_suffix()}",
                            ],
                        )
                        return

                self.state = "ACTIVE"
                self.battle.run_count += 1

                # Assign LEAD / CHASE based on spline position
                if self.battle.run_count == 1:
                    delta = (car1.spline - car2.spline) % 1.0
                    if delta < 0.5:
                        self.battle.lead_guid  = car1.guid
                        self.battle.chase_guid = car2.guid
                    else:
                        self.battle.lead_guid  = car2.guid
                        self.battle.chase_guid = car1.guid
                else:
                    # Alternate roles on subsequent runs
                    self.battle.lead_guid, self.battle.chase_guid = self.battle.chase_guid, self.battle.lead_guid

                car1.driven_spline = 0.0
                car2.driven_spline = 0.0
                self.active_start_time = now

                lead_car  = self.cars[self.battle.lead_guid]
                chase_car = self.cars[self.battle.chase_guid]

                gap = (lead_car.spline - chase_car.spline) % 1.0
                self.battle.initial_gap_spline = gap if gap < 0.5 else 0.0

                print(f"🔥 [BATTLE] ACTIVE — RUN #{self.battle.run_count}")
                print(f"   🚩 LEAD:  {self.battle.lead_guid}")
                print(f"   🦊 CHASE: {self.battle.chase_guid} | Initial gap: {self.battle.initial_gap_spline:.4f} spline")
                
                # Send starting message to both players regarding their position
                if self.on_chat_message:
                    self.on_chat_message(self.battle.lead_guid, "[TOUGE] LEAD")
                    self.on_chat_message(self.battle.chase_guid, "[TOUGE] CHASE")

                # Emit live score webhook to notify frontend the run started
                if self.on_score_update:
                    self.on_score_update(self.battle_id, self.battle.car1_score, self.battle.car2_score, None, self.battle.points_log)

            elif now - self.launch_trigger_time > 3.0:
                print("[BATTLE] Timeout: opponent did not reach 40 km/h within 3s. Cancelling.")
                if self.on_chat_message:
                    msg = "[TOUGE] T-out launch"
                    self.on_chat_message(car1.guid, msg)
                    self.on_chat_message(car2.guid, msg)
                self._reset_to_idle()

        # ==========================
        # ACTIVE: Battle in progress
        # ==========================
        elif self.state == "ACTIVE":
            lead_car  = self.cars[self.battle.lead_guid]
            chase_car = self.cars[self.battle.chase_guid]

            if distance >= OUTRUN_GAP_AUTO_POINT_METERS and not TEST_MODE_1_PLAYER:
                print(
                    f"🏁 [BATTLE] OUTRUN AUTO — gap {distance:.1f}m >= "
                    f"{OUTRUN_GAP_AUTO_POINT_METERS:.1f}m. Point for LEAD!"
                )
                self._award_point(self.battle.lead_guid, reason='outrun_gap')
                return

            # 1. OVERTAKE: Chase passes Lead cleanly
            # Wait at least 2 seconds after LAUNCH to prevent instant overtakes from parallel rolling starts
            if (now - self.active_start_time) > 2.0:
                if chase_car.driven_spline > (lead_car.driven_spline + self.battle.initial_gap_spline + self.overtake_margin_spline):
                    print(f"🏎️💨 [BATTLE] OVERTAKE! CHASE ({self.battle.chase_guid}) cleanly passed the LEAD!")
                    self._award_point(self.battle.chase_guid, reason='overtake')
                    return

            # 2. FINISH: Lead reached the virtual finish line
            if lead_car.driven_spline >= self.run_length_spline:
                chase_gap = (lead_car.spline - chase_car.spline) % 1.0
                is_draw = chase_gap <= self.judge_offset_spline or chase_gap > 0.5

                if is_draw:
                    print(f"🏁 [BATTLE] FINISH — DRAW. Chase gap: {chase_gap:.4f} spline")
                    self._award_point(None, reason='draw')
                else:
                    print(f"🏁 [BATTLE] FINISH — OUTRUN. Gap: {chase_gap:.4f}. Point for LEAD!")
                    self._award_point(self.battle.lead_guid)

    def _award_point(self, winner_guid, reason='outrun'):
        import time as _time

        def _notify_both(msg):
            if self.on_chat_message:
                self.on_chat_message(self.battle.car1_guid, msg)
                self.on_chat_message(self.battle.car2_guid, msg)

        if winner_guid == self.battle.car1_guid:
            self.battle.car1_score += 1
            log_msg = f"Point to {self.battle.car1_guid} ({reason})"
        elif winner_guid == self.battle.car2_guid:
            self.battle.car2_score += 1
            log_msg = f"Point to {self.battle.car2_guid} ({reason})"
        else:
            log_msg = f"DRAW ({reason})"

        self.battle.points_log.append({
            'scorer': winner_guid,
            'reason': reason,
            'ts': int(_time.time() * 1000)
        })

        print(f"🏅 {log_msg}. Score: {self.battle.car1_score} - {self.battle.car2_score}")

        _notify_both(self._format_point_broadcast(winner_guid, reason))

        series_done = False
        if self.battle.car1_score >= POINTS_TO_WIN:
            self.battle.winner = self.battle.car1_guid
            series_done = True
            print(f"🏆 [BATTLE] SERIES OVER! WINNER: {self.battle.winner}")
        elif self.battle.car2_score >= POINTS_TO_WIN:
            self.battle.winner = self.battle.car2_guid
            series_done = True
            print(f"🏆 [BATTLE] SERIES OVER! WINNER: {self.battle.winner}")

        if series_done:
            wn = self._display_name(self.battle.winner)
            _notify_both(f"[TOUGE] WIN {wn} | {self._scoreboard_line()}")

        self.state = "WAITING_RESTART"
        print(
            f"⏳ [BATTLE] Session restart in {POINT_TO_PITS_DELAY_SEC:.1f}s "
            f"(no countdown message to players)"
        )

        if series_done:
            if self.on_score_update:
                self.on_score_update(
                    self.battle_id,
                    self.battle.car1_score,
                    self.battle.car2_score,
                    self.battle.winner,
                    self.battle.points_log,
                )
            self._schedule_end_run(is_series_end=True, set_finished_after=True)
        else:
            if self.on_score_update:
                self.on_score_update(
                    self.battle_id,
                    self.battle.car1_score,
                    self.battle.car2_score,
                    None,
                    self.battle.points_log,
                )
            self._schedule_end_run(is_series_end=False)

    def _schedule_end_run(self, is_series_end=False, set_finished_after=False):
        """Tras mostrar mensajes de punto, espera y luego pide restart a pits."""
        self._cancel_restart_timer()
        delay = max(0.0, POINT_TO_PITS_DELAY_SEC)

        def _fire():
            self._restart_timer = None
            if self.state != "WAITING_RESTART":
                return
            self._end_run(is_series_end)
            if set_finished_after:
                self.state = "FINISHED"

        self._restart_timer = threading.Timer(delay, _fire)
        self._restart_timer.daemon = True
        self._restart_timer.start()

    def _abort_run_no_point(self, reason, chat_sequence):
        """
        Aborts run with no point: send chat_sequence (English), then silent delay, then restart.
        """
        print(f"\n⚠️ [BATTLE] Run aborted ({reason}). No point awarded.")
        self._send_chat_sequence(chat_sequence)
        self.state = "WAITING_RESTART"
        print(
            f"⏳ [BATTLE] Session restart in {POINT_TO_PITS_DELAY_SEC:.1f}s "
            f"(abort; no countdown message to players)"
        )
        self._schedule_end_run(is_series_end=False, set_finished_after=False)


    def _end_run(self, is_series_end=False):
        """Returns to IDLE/FINISHED and requests a server session restart."""
        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0
        for c in self.cars.values():
            c.driven_spline = 0.0

        if is_series_end:
            print("🔄 [BATTLE] Series finished. Restarting session to send players to pits...")
        else:
            print("🔄 [BATTLE] Run complete. Restarting session to return players to pits...")

        if self.on_session_restart:
            try:
                self.on_session_restart()
                self._gap_abort_suppressed_until = time.time() + POST_RESTART_GAP_GRACE_SEC
                self._restart_settle_until = time.time() + RESTART_SETTLE_SEC
                self.state = "RESTARTING"
            except Exception as e:
                print(f"❌ [BATTLE] Failed to request /restart_session: {e}")

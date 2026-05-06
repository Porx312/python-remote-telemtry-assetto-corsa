from engines.battlesystem.orchestrator import BattleManager
from engines.battlesystem.pair_manager import PairBattleManager

__all__ = ["BattleManager", "PairBattleManager"]
from engines.battlesystem.orchestrator import BattleManager
from engines.battlesystem.pair_manager import PairBattleManager

__all__ = ["BattleManager", "PairBattleManager"]
import math
import time
from engines.battlesystem.config import (
    BRAKE_CHECK_DELTA_KMH,
    BRAKE_CHECK_LOW_SPEED_KMH,
    BRAKE_CHECK_MIN_IMPACT,
    COLLISION_MIN_COMBINED_SPEED_KMH,
    COLLISION_MIN_IMPACT,
    COLLISION_MIN_REL_SPEED,
    COLLISION_POINT_COOLDOWN_SEC,
    COLLISION_WARN_COOLDOWN_SEC,
    DEFAULT_WIN_MIN_POINTS,
    DISAPPEAR_GAP_METERS,
    FINISH_POINT_MIN_GAP_METERS,
    GAP_ABORT_MIN_BOTH_SPEED_KMH,
    MAX_BATTLE_GAP_METERS,
    OVERTAKE_MARGIN_SPLINE,
    OVERTAKE_MIN_GAP_METERS,
    OVERTAKE_POINT_COOLDOWN_SEC,
    PAIR_LOCK_MAX_DISTANCE_METERS,
    PAIR_LOCK_MIN_SPEED_KMH,
    PAIR_STICKY_TIMEOUT_SEC,
    PRESTART_GAP_ABORT_GRACE_SEC,
    ROLE_ASSIGN_MIN_GAP_SPLINE,
    ROLE_ASSIGN_WAIT_SEC,
    WRONG_POSITION_CHECK_WINDOW_SEC,
    WRONG_POSITION_MARGIN_SPLINE,
)
from engines.battlesystem.models import CarState, TougeBattle


class PairBattleManager:
    """
    Manages the full Touge battle state machine:
    IDLE -> ARMED -> LAUNCHING -> ACTIVE -> FINISHED
    """
    def __init__(self):
        self.state = "IDLE"
        self.cars = {}      # guid -> CarState
        self.battle = None  # TougeBattle instance
        self.is_battle_server = False

        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0

        # Callbacks set by main.py to        # Callbacks to emit events to main.py
        self.on_battle_start = None      # kwargs: (car1_guid, car2_guid) -> returns battle_id
        self.on_score_update = None      # kwargs: (battle_id, p1_score, p2_score, winner_guid, log)
        self.on_chat_message = None      # kwargs: (guid, msg) -> sends /chat to specific user
        
        # State
        self.battle = None

        self.battle_id = None  # DB row id of the current active battle

        # Auto-reset after FINISHED
        self.finished_time = 0.0
        self.FINISHED_COOLDOWN = 10.0  # Seconds before accepting a new battle

        # Battle config
        # Single continuous run: close only after completing the full lap.
        self.run_length_spline = 1.0
        self.judge_offset_spline = 0.03 # Draw tolerance
        self.overtake_margin_spline = OVERTAKE_MARGIN_SPLINE
        self.active_start_time = 0.0
        # guid -> display name (from telemetry); used in scoreboard chat lines
        self.player_names = {}
        self._overtake_chase_scored = False
        self._last_overtake_point_ts = 0.0
        self._last_collision_warn_ts = 0.0
        self._last_collision_point_ts = 0.0

    def set_server_mode(self, is_battle_server):
        is_battle = bool(is_battle_server)
        if self.is_battle_server == is_battle:
            return
        self.is_battle_server = is_battle
        if not self.is_battle_server:
            # Hard stop any in-flight series when server changes to non-battle mode.
            self._reset_to_idle(full_reset=True)
            self.battle = None

    def _pick_candidate_pair(self, active_guids):
        if len(active_guids) < 2:
            return None
        best_pair = None
        best_distance = None
        for i in range(len(active_guids)):
            for j in range(i + 1, len(active_guids)):
                g1 = active_guids[i]
                g2 = active_guids[j]
                c1 = self.cars.get(g1)
                c2 = self.cars.get(g2)
                if not c1 or not c2:
                    continue
                distance = self.get_distance(c1.pos, c2.pos)
                if distance > PAIR_LOCK_MAX_DISTANCE_METERS:
                    continue
                if c1.speed < PAIR_LOCK_MIN_SPEED_KMH or c2.speed < PAIR_LOCK_MIN_SPEED_KMH:
                    continue
                if best_distance is None or distance < best_distance:
                    best_distance = distance
                    best_pair = (g1, g2)
        return best_pair

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

    def _score_of(self, guid):
        if not self.battle:
            return 0
        if guid == self.battle.car1_guid:
            return self.battle.car1_score
        if guid == self.battle.car2_guid:
            return self.battle.car2_score
        return 0

    def _finalize_default_win(self, winner_guid, reason):
        if self.state != "ACTIVE" or not self.battle or not winner_guid:
            return False
        winner_points = self._score_of(winner_guid)
        if winner_points < DEFAULT_WIN_MIN_POINTS:
            print(
                f"⚠️ [BATTLE] Default win skipped ({winner_points} < {DEFAULT_WIN_MIN_POINTS}) "
                f"| reason={reason}"
            )
            self._reset_to_idle(full_reset=False)
            return True

        self.battle.winner = winner_guid
        wn = self._display_name(winner_guid)
        print(
            f"🏆 [BATTLE] DEFAULT WIN {winner_guid} | reason={reason} | "
            f"score={self.battle.car1_score}-{self.battle.car2_score}"
        )
        if self.on_chat_message:
            msg = f"[TOUGE] WIN {wn} DEFAULT ({reason}) | {self._scoreboard_line()}"
            self.on_chat_message(self.battle.car1_guid, msg)
            self.on_chat_message(self.battle.car2_guid, msg)
        if self.on_score_update:
            self.on_score_update(
                self.battle_id,
                self.battle.car1_score,
                self.battle.car2_score,
                self.battle.winner,
                self.battle.points_log,
                self.battle.car1_guid,
                self.battle.car2_guid,
            )
        self.state = "FINISHED"
        return True

    def _format_point_broadcast(self, winner_guid, reason):
        board = self._scoreboard_line()
        pit = ""
        if reason == "draw":
            return f"[TOUGE] DRAW | {board}{pit}"
        if reason == "overtake":
            return f"[TOUGE] OVERTAKE | {board}{pit}"
        if reason == "position_recovery":
            return f"[TOUGE] RECOVER | {board}{pit}"
        if reason == "outrun":
            return f"[TOUGE] OUTRUN | {board}{pit}"
        if reason == "dnf_lead_stalled":
            return f"[TOUGE] DNF lead | {board}{pit}"
        if reason == "dnf_chase_stalled":
            return f"[TOUGE] DNF chase | {board}{pit}"
        if reason == "collision_penalty":
            return f"[TOUGE] HIT rear | {board}{pit}"
        if reason == "collision_brake_check":
            return f"[TOUGE] HIT brake | {board}{pit}"
        return f"[TOUGE] PT {reason} | {board}{pit}"

    def _pit_suffix(self):
        # Kept only for legacy message strings; pits flow is disabled.
        return ""

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

    def _notify_battle_cancelled(self, reason=None):
        if not self.on_chat_message or not self.battle:
            return
        if reason:
            msg = f"[TOUGE] BATTLE CANCELLED ({reason})"
        else:
            msg = "[TOUGE] BATTLE CANCELLED"
        self.on_chat_message(self.battle.car1_guid, msg)
        self.on_chat_message(self.battle.car2_guid, msg)

    def get_distance(self, pos1, pos2):
        return math.sqrt((pos1[0]-pos2[0])**2 + (pos1[1]-pos2[1])**2 + (pos1[2]-pos2[2])**2)

    def update(self, driver_guid, spline, speed, world_position):
        """Called on every CAR_UPDATE packet (packet 53) from the server."""
        if not self.is_battle_server:
            return
        if driver_guid not in self.cars:
            self.cars[driver_guid] = CarState(driver_guid)
        self.cars[driver_guid].update(spline, speed, world_position)
        try:
            self._process_logic()
        except Exception as e:
            print(f"[BATTLE] Logic error (non-fatal): {e}")

    def remove_car(self, driver_guid):
        """Called when a player disconnects."""
        if driver_guid in self.cars:
            del self.cars[driver_guid]
        if self.state in ["ARMED", "LAUNCHING", "ACTIVE"]:
            print(f"[BATTLE] Player {driver_guid} disconnected. Cancelling battle.")
            self._reset_to_idle()

    def _reset_to_idle(self, full_reset=False):
        self.state = "IDLE"
        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0
        self.finished_time = 0.0
        if full_reset:
            self.battle_id = None

    def handle_collision(self, car1_guid, car2_guid, impact_speed):
        """Called by main.py on CE_COLLISION_WITH_CAR."""
        if not self.is_battle_server:
            return
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
        relative_speed = abs(chase_car.speed - lead_car.speed)
        combined_speed = lead_car.speed + chase_car.speed
        now_ts = time.time()

        # Always warn players (rate-limited), even if no point awarded.
        print(
            f"💥 [BATTLE] Collision noted (no point). Impact: {impact_speed:.2f}, "
            f"Δspeed: {relative_speed:.1f} (Lead: {lead_car.speed:.1f}, Chase: {chase_car.speed:.1f})"
        )
        if self.on_chat_message and (now_ts - self._last_collision_warn_ts) >= COLLISION_WARN_COOLDOWN_SEC:
            warn = "[TOUGE] WARNING collision detected. Repeated impacts may cost points."
            self.on_chat_message(self.battle.car1_guid, warn)
            self.on_chat_message(self.battle.car2_guid, warn)
            self._last_collision_warn_ts = now_ts

        # Point debounce: avoid counting the same impact chain repeatedly.
        if (now_ts - self._last_collision_point_ts) < COLLISION_POINT_COOLDOWN_SEC:
            return

        # Ignore very light contacts.
        if impact_speed < COLLISION_MIN_IMPACT and relative_speed < COLLISION_MIN_REL_SPEED:
            return
        if combined_speed < COLLISION_MIN_COMBINED_SPEED_KMH:
            return

        # Fault logic:
        # - Brake check: LEAD is abnormally slow + CHASE closing fast + strong impact -> LEAD at fault.
        # - Otherwise rear-end: CHASE at fault.
        lead_is_abnormally_slow = lead_car.speed <= BRAKE_CHECK_LOW_SPEED_KMH
        chase_closing_fast = (chase_car.speed - lead_car.speed) >= BRAKE_CHECK_DELTA_KMH
        strong_impact = impact_speed >= BRAKE_CHECK_MIN_IMPACT
        self._last_collision_point_ts = now_ts
        if strong_impact and lead_is_abnormally_slow and chase_closing_fast:
            print(
                f"💥 [BATTLE] BRAKE CHECK PENALTY! Lead caused crash. "
                f"Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f}, Chase: {chase_car.speed:.1f})"
            )
            self._award_point(self.battle.chase_guid, reason='collision_brake_check')
        else:
            print(
                f"💥 [BATTLE] COLLISION Penalty! Chase hit Lead. "
                f"Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f}, Chase: {chase_car.speed:.1f})"
            )
            self._award_point(self.battle.lead_guid, reason='collision_penalty')

    def _process_logic(self):
        now = time.time()
        if not self.is_battle_server:
            return

        # Only consider cars that have sent telemetry in the last 5 seconds
        active_guids = [g for g, c in self.cars.items() if (now - c.last_update_time) < 5.0]

        min_players = 2

        if len(active_guids) < min_players:
            if self.state not in ["IDLE", "FINISHED"]:
                self._notify_battle_cancelled("not enough players")
                print(f"\n[BATTLE] Not enough players ({len(active_guids)}). Resetting.")
                self._reset_to_idle(full_reset=True)
            return

        if self.battle:
            # Never switch to a different pair while one battle is already tracked.
            # Only cancel after a generous stale timeout (or explicit disconnect in remove_car()).
            p1 = self.cars.get(self.battle.car1_guid)
            p2 = self.cars.get(self.battle.car2_guid)
            if not p1 or not p2:
                if self.state not in ["IDLE", "FINISHED"]:
                    self._notify_battle_cancelled("pair missing")
                    print("\n[BATTLE] Active pair missing from car state. Resetting.")
                self._reset_to_idle(full_reset=True)
                self.battle = None
                return
            p1_stale = (now - p1.last_update_time) > PAIR_STICKY_TIMEOUT_SEC
            p2_stale = (now - p2.last_update_time) > PAIR_STICKY_TIMEOUT_SEC
            if p1_stale or p2_stale:
                if self.state == "ACTIVE":
                    remaining_guid = None
                    if p1_stale and not p2_stale:
                        remaining_guid = self.battle.car2_guid
                    elif p2_stale and not p1_stale:
                        remaining_guid = self.battle.car1_guid
                    if remaining_guid and self._finalize_default_win(remaining_guid, "opponent_disconnected"):
                        return
                if self.state not in ["IDLE", "FINISHED"]:
                    self._notify_battle_cancelled("pair stale")
                    print("\n[BATTLE] Active pair stale timeout reached. Resetting.")
                self._reset_to_idle(full_reset=True)
                self.battle = None
                return
            # Pause logic while waiting fresh telemetry from one of the two locked drivers.
            if self.battle.car1_guid not in active_guids or self.battle.car2_guid not in active_guids:
                return
        else:
            pair = self._pick_candidate_pair(active_guids)
            if not pair:
                return
            self.battle = TougeBattle(pair[0], pair[1])
            self._reset_to_idle(full_reset=True)

        if self.state == "FINISHED":
            # Auto-reset after cooldown so a new battle can begin
            if self.finished_time == 0.0:
                self.finished_time = now
            elif now - self.finished_time >= self.FINISHED_COOLDOWN:
                car1 = self.cars[self.battle.car1_guid]
                car2 = self.cars[self.battle.car2_guid]
                # Enforce that drivers must slow down (< 20kmh) or return to the start (< 0.1 spline)
                # before the battle engine will arm a new round, preventing accidental high-speed false starts.
                if (car1.speed < 20.0 and car2.speed < 20.0) or (car1.spline < 0.1 and car2.spline < 0.1):
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
            close_enough = distance < 40.0
            if close_enough:
                # If they are cruising together, wait for someone to gun it
                if car1.speed >= 25.0 or car2.speed >= 25.0:
                    self.state = "ARMED"
                    self.condition_start_time = now
                    print(f"⚡ [BATTLE] ARMED between {car1.guid} and {car2.guid}!")
                    if self.on_chat_message:
                        msg = (
                            f"[TOUGE] {self._display_name(car1.guid)} vs "
                            f"{self._display_name(car2.guid)} | ARMED"
                        )
                        self.on_chat_message(car1.guid, msg)
                        self.on_chat_message(car2.guid, msg)

        # ==========================
        # ARMED: Waiting for both cars to hit 40 km/h
        # ==========================
        elif self.state == "ARMED":
            # Still loading / 0 km/h in pits: positions can look "far apart" — don't abort yet
            both_moving = (
                car1.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH
                and car2.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH
            )
            if (
                distance > MAX_BATTLE_GAP_METERS
                
                and both_moving
                and (now - self.condition_start_time) >= PRESTART_GAP_ABORT_GRACE_SEC
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
            both_moving = (
                car1.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH
                and car2.speed >= GAP_ABORT_MIN_BOTH_SPEED_KMH
            )
            if (
                distance > MAX_BATTLE_GAP_METERS
                
                and both_moving
                and (now - self.launch_trigger_time) >= PRESTART_GAP_ABORT_GRACE_SEC
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
                else:
                    # Run #1: do not decide roles while fully side-by-side.
                    c1_ahead_gap = (car1.spline - car2.spline) % 1.0
                    c2_ahead_gap = (car2.spline - car1.spline) % 1.0
                    clear_gap = min(c1_ahead_gap, c2_ahead_gap)
                    if clear_gap < ROLE_ASSIGN_MIN_GAP_SPLINE:
                        if (now - self.launch_trigger_time) <= ROLE_ASSIGN_WAIT_SEC:
                            return
                        self._abort_run_no_point(
                            "leader_not_clear",
                            [f"[TOUGE] Leader not clear{self._pit_suffix()}"],
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
                self._overtake_chase_scored = False
                self._last_overtake_point_ts = 0.0

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

            # If gap explodes (teleport/pits/disappear), grant default win only when scorer has >= threshold points.
            if distance >= DISAPPEAR_GAP_METERS:
                if lead_car.driven_spline >= chase_car.driven_spline:
                    survivor_guid = self.battle.lead_guid
                else:
                    survivor_guid = self.battle.chase_guid
                if self._finalize_default_win(survivor_guid, "gap_disappeared"):
                    return

            # Runs after role swap: CHASE must not be ahead right after launch.
            if self.battle.run_count > 1 and (now - self.active_start_time) <= WRONG_POSITION_CHECK_WINDOW_SEC:
                if chase_car.driven_spline > (lead_car.driven_spline + WRONG_POSITION_MARGIN_SPLINE):
                    order_line = (
                        f"L {self._display_name(self.battle.lead_guid)} / "
                        f"C {self._display_name(self.battle.chase_guid)}"
                    )
                    self._abort_run_no_point(
                        "wrong_position",
                        [
                            (self.battle.chase_guid, f"[TOUGE] YOU ARE NOT THE LEADER | {order_line}{self._pit_suffix()}"),
                            (self.battle.lead_guid, f"[TOUGE] Opponent wrong position | {order_line}{self._pit_suffix()}"),
                            f"[TOUGE] Wrong position no PT | {order_line}{self._pit_suffix()}",
                        ],
                    )
                    return

            # OVERTAKE point: one clean pass by CHASE per run.
            if (now - self.active_start_time) > 2.0:
                if (now - self._last_overtake_point_ts) >= OVERTAKE_POINT_COOLDOWN_SEC:
                    # Overtake/recovery only counts when gap is clearly opened.
                    if not self._overtake_chase_scored:
                        required_gap = self.battle.initial_gap_spline + self.overtake_margin_spline
                        if (
                            chase_car.driven_spline > (lead_car.driven_spline + required_gap)
                            and distance >= OVERTAKE_MIN_GAP_METERS
                        ):
                            print(
                                f"🏎️💨 [BATTLE] OVERTAKE! CHASE ({self.battle.chase_guid}) "
                                f"cleanly passed LEAD (gap {distance:.1f}m >= {OVERTAKE_MIN_GAP_METERS:.1f}m)."
                            )
                            self._overtake_chase_scored = True
                            self._last_overtake_point_ts = now
                            self._award_point(self.battle.chase_guid, reason='overtake')
                            return
                    else:
                        # Recovery point for LEAD after CHASE already scored overtake in this run.
                        if (
                            lead_car.driven_spline > (chase_car.driven_spline + self.overtake_margin_spline)
                            and distance >= OVERTAKE_MIN_GAP_METERS
                        ):
                            print(
                                f"🔁 [BATTLE] RECOVERY! LEAD ({self.battle.lead_guid}) "
                                f"recovered position (gap {distance:.1f}m >= {OVERTAKE_MIN_GAP_METERS:.1f}m)."
                            )
                            self._overtake_chase_scored = False
                            self._last_overtake_point_ts = now
                            self._award_point(self.battle.lead_guid, reason='position_recovery')
                            return

            # Only finish point counts in this mode.
            # FINISH: Lead reached the virtual finish line
            if lead_car.driven_spline >= self.run_length_spline:
                finish_gap_m = self.get_distance(lead_car.pos, chase_car.pos)
                is_draw = finish_gap_m < FINISH_POINT_MIN_GAP_METERS
                if is_draw:
                    print(
                        f"🏁 [BATTLE] FINISH — DRAW. Gap {finish_gap_m:.1f}m < "
                        f"{FINISH_POINT_MIN_GAP_METERS:.1f}m"
                    )
                else:
                    print(
                        f"🏁 [BATTLE] FINISH — POINT LEAD. Gap {finish_gap_m:.1f}m >= "
                        f"{FINISH_POINT_MIN_GAP_METERS:.1f}m"
                    )
                    self._award_point(self.battle.lead_guid, reason='finish_outrun')
                self._finalize_single_session_result(finish_gap_m, is_draw)
                return

    def _finalize_single_session_result(self, finish_gap_m, is_draw):
        """Finalize a continuous points run at 100% track completion."""
        if self.state != "ACTIVE":
            return
        if self.battle.car1_score > self.battle.car2_score:
            winner = self.battle.car1_guid
        elif self.battle.car2_score > self.battle.car1_score:
            winner = self.battle.car2_guid
        else:
            # True draw: similar finish distance => no winner.
            winner = None

        self.battle.winner = winner
        if winner:
            wn = self._display_name(winner)
            print(f"🏆 [BATTLE] SINGLE SESSION OVER! WINNER: {winner}")
            msg = f"[TOUGE] WIN {wn} | {self._scoreboard_line()}"
        else:
            print(
                f"🤝 [BATTLE] SINGLE SESSION OVER! DRAW "
                f"(finish gap {finish_gap_m:.1f}m < {FINISH_POINT_MIN_GAP_METERS:.1f}m)"
            )
            msg = f"[TOUGE] DRAW FINAL | {self._scoreboard_line()}"
        if self.on_chat_message:
            self.on_chat_message(self.battle.car1_guid, msg)
            self.on_chat_message(self.battle.car2_guid, msg)
        if self.on_score_update:
            self.on_score_update(
                self.battle_id,
                self.battle.car1_score,
                self.battle.car2_score,
                self.battle.winner,
                self.battle.points_log,
                self.battle.car1_guid,
                self.battle.car2_guid,
            )
        self.state = "FINISHED"

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

        # Single-session mode keeps accumulating points until 100% finish.
        return

    def _abort_run_no_point(self, reason, chat_sequence):
        """
        Abort run without point and continue immediately (no pits/restart flow).
        """
        print(f"\n⚠️ [BATTLE] Run aborted ({reason}). No point awarded.")
        self._send_chat_sequence(chat_sequence)
        self._notify_battle_cancelled(reason)
        self.state = "IDLE"


class BattleManager:
    """
    Orchestrates multiple concurrent 1v1 touge battles on open servers.
    Each active pair runs an isolated PairBattleManager state machine.
    """

    def __init__(self):
        self.is_battle_server = False
        self.cars = {}  # guid -> CarState (global telemetry cache)
        self.player_names = {}
        self.pair_managers = {}  # (guid_a, guid_b) sorted -> PairBattleManager
        self.guid_to_pair = {}  # guid -> pair key

        # External callbacks (same contract as legacy manager).
        self.on_battle_start = None
        self.on_score_update = None
        self.on_chat_message = None

    @staticmethod
    def _pair_key(g1, g2):
        return tuple(sorted((g1, g2)))

    def _build_pair_manager(self, g1, g2):
        mgr = PairBattleManager()
        mgr.set_server_mode(True)
        mgr.battle = TougeBattle(g1, g2)
        mgr._reset_to_idle(full_reset=True)
        mgr.player_names[g1] = self.player_names.get(g1, g1)
        mgr.player_names[g2] = self.player_names.get(g2, g2)
        # Callbacks are proxied to server_state handlers.
        mgr.on_battle_start = self.on_battle_start
        mgr.on_score_update = self.on_score_update
        mgr.on_chat_message = self.on_chat_message
        return mgr

    def _cleanup_pair_if_done(self, key):
        mgr = self.pair_managers.get(key)
        if not mgr:
            return
        b = mgr.battle
        if not b:
            return
        p1 = mgr.cars.get(b.car1_guid)
        p2 = mgr.cars.get(b.car2_guid)
        # If either participant vanished from this sub-manager, drop pair mapping.
        if not p1 or not p2:
            self.pair_managers.pop(key, None)
            for g in key:
                if self.guid_to_pair.get(g) == key:
                    self.guid_to_pair.pop(g, None)

    def _try_matchmake(self):
        now = time.time()
        # Candidates not currently locked to any pair and recently active.
        free = []
        for g, c in self.cars.items():
            if (now - c.last_update_time) > 3.0:
                continue
            if g in self.guid_to_pair:
                continue
            free.append(g)
        if len(free) < 2:
            return

        # Greedy nearest-neighbor matching under lock constraints.
        while len(free) >= 2:
            best = None
            best_dist = None
            for i in range(len(free)):
                for j in range(i + 1, len(free)):
                    g1 = free[i]
                    g2 = free[j]
                    c1 = self.cars.get(g1)
                    c2 = self.cars.get(g2)
                    if not c1 or not c2:
                        continue
                    if c1.speed < PAIR_LOCK_MIN_SPEED_KMH or c2.speed < PAIR_LOCK_MIN_SPEED_KMH:
                        continue
                    dist = math.sqrt(
                        (c1.pos[0] - c2.pos[0]) ** 2
                        + (c1.pos[1] - c2.pos[1]) ** 2
                        + (c1.pos[2] - c2.pos[2]) ** 2
                    )
                    if dist > PAIR_LOCK_MAX_DISTANCE_METERS:
                        continue
                    if best_dist is None or dist < best_dist:
                        best_dist = dist
                        best = (g1, g2)
            if not best:
                return

            g1, g2 = best
            key = self._pair_key(g1, g2)
            if key not in self.pair_managers:
                mgr = self._build_pair_manager(g1, g2)
                self.pair_managers[key] = mgr
                self.guid_to_pair[g1] = key
                self.guid_to_pair[g2] = key
                print(
                    f"🤝 [BATTLE] Pair locked: {mgr._display_name(g1)} vs {mgr._display_name(g2)} "
                    f"(active pairs: {len(self.pair_managers)})"
                )
            free = [g for g in free if g not in (g1, g2)]

    def set_server_mode(self, is_battle_server):
        is_battle = bool(is_battle_server)
        if self.is_battle_server == is_battle:
            return
        self.is_battle_server = is_battle
        if not self.is_battle_server:
            self.pair_managers.clear()
            self.guid_to_pair.clear()

    def set_driver_name(self, guid, name):
        if not guid or not name or str(guid).startswith("unknown"):
            return
        self.player_names[guid] = str(name).strip()
        key = self.guid_to_pair.get(guid)
        if key and key in self.pair_managers:
            self.pair_managers[key].set_driver_name(guid, name)

    def update(self, driver_guid, spline, speed, world_position):
        if not self.is_battle_server:
            return
        if driver_guid not in self.cars:
            self.cars[driver_guid] = CarState(driver_guid)
        self.cars[driver_guid].update(spline, speed, world_position)

        self._try_matchmake()
        key = self.guid_to_pair.get(driver_guid)
        if not key:
            return
        mgr = self.pair_managers.get(key)
        if not mgr:
            self.guid_to_pair.pop(driver_guid, None)
            return

        # Mirror both participants latest telemetry into pair manager.
        b = mgr.battle
        if not b:
            return
        for guid in (b.car1_guid, b.car2_guid):
            c = self.cars.get(guid)
            if not c:
                continue
            if guid not in mgr.cars:
                mgr.cars[guid] = CarState(guid)
            mgr.cars[guid].update(c.spline, c.speed, c.pos)
            n = self.player_names.get(guid)
            if n:
                mgr.player_names[guid] = n

        try:
            mgr._process_logic()
        except Exception as e:
            print(f"[BATTLE] Pair logic error (non-fatal): {e}")

        self._cleanup_pair_if_done(key)

    def handle_collision(self, car1_guid, car2_guid, impact_speed):
        if not self.is_battle_server:
            return
        key1 = self.guid_to_pair.get(car1_guid)
        key2 = self.guid_to_pair.get(car2_guid)
        if not key1 or key1 != key2:
            return
        mgr = self.pair_managers.get(key1)
        if not mgr:
            return
        mgr.handle_collision(car1_guid, car2_guid, impact_speed)

    def remove_car(self, driver_guid):
        self.cars.pop(driver_guid, None)
        self.player_names.pop(driver_guid, None)
        key = self.guid_to_pair.pop(driver_guid, None)
        if not key:
            return
        mgr = self.pair_managers.pop(key, None)
        if not mgr:
            return
        other = key[0] if key[1] == driver_guid else key[1]
        self.guid_to_pair.pop(other, None)
        mgr.remove_car(driver_guid)

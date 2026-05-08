import math
import time

from engines.battlesystem.chat import (
    format_point_broadcast,
    notify_battle_cancelled,
)
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
from engines.battlesystem.scoring import (
    award_point,
    finalize_default_win,
    finalize_single_session_result,
    score_of,
)
from engines.battlesystem.state_machine import process_pair_logic


class PairBattleManager:
    """
    Manages the full Touge battle state machine:
    IDLE -> ARMED -> LAUNCHING -> ACTIVE -> FINISHED
    """

    def __init__(self):
        self.state = "IDLE"
        self.cars = {}
        self.battle = None
        self.is_battle_server = False

        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0

        self.on_battle_start = None
        self.on_score_update = None
        self.on_chat_message = None

        self.battle = None
        self.battle_id = None
        self.finished_time = 0.0
        self.FINISHED_COOLDOWN = 10.0
        self.run_length_spline = 1.0
        self.judge_offset_spline = 0.03
        self.overtake_margin_spline = OVERTAKE_MARGIN_SPLINE
        self.active_start_time = 0.0
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
        return f"{self._display_name(g1)} {self.battle.car1_score} : {self._display_name(g2)} {self.battle.car2_score}"

    def _score_of(self, guid):
        return score_of(self, guid)

    def _finalize_default_win(self, winner_guid, reason):
        return finalize_default_win(self, winner_guid, reason)

    def _format_point_broadcast(self, winner_guid, reason):
        return format_point_broadcast(self, winner_guid, reason)

    def _notify_battle_cancelled(self, reason=None):
        notify_battle_cancelled(self, reason)

    def get_distance(self, pos1, pos2):
        return math.sqrt((pos1[0] - pos2[0]) ** 2 + (pos1[1] - pos2[1]) ** 2 + (pos1[2] - pos2[2]) ** 2)

    def update(self, driver_guid, spline, speed, world_position):
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
        if not self.is_battle_server or self.state != "ACTIVE":
            return
        if not self.battle or self.battle.car1_guid == self.battle.car2_guid:
            return

        guids = {car1_guid, car2_guid}
        if self.battle.car1_guid not in guids or self.battle.car2_guid not in guids:
            print(
                f"⚠️ [BATTLE] Collision ignored: participants mismatch ({car1_guid}, {car2_guid}) vs expected "
                f"({self.battle.car1_guid}, {self.battle.car2_guid})"
            )
            return

        lead_car = self.cars[self.battle.lead_guid]
        chase_car = self.cars[self.battle.chase_guid]
        relative_speed = abs(chase_car.speed - lead_car.speed)
        combined_speed = lead_car.speed + chase_car.speed
        now_ts = time.time()
        print(
            f"💥 [BATTLE] Collision noted (no point). Impact: {impact_speed:.2f}, "
            f"Δspeed: {relative_speed:.1f} (Lead: {lead_car.speed:.1f}, Chase: {chase_car.speed:.1f})"
        )
        if self.on_chat_message and (now_ts - self._last_collision_warn_ts) >= COLLISION_WARN_COOLDOWN_SEC:
            warn = "[TOUGE] WARNING collision detected. Repeated impacts may cost points."
            self.on_chat_message(self.battle.car1_guid, warn)
            self.on_chat_message(self.battle.car2_guid, warn)
            self._last_collision_warn_ts = now_ts

        if (now_ts - self._last_collision_point_ts) < COLLISION_POINT_COOLDOWN_SEC:
            return
        if impact_speed < COLLISION_MIN_IMPACT and relative_speed < COLLISION_MIN_REL_SPEED:
            return
        if combined_speed < COLLISION_MIN_COMBINED_SPEED_KMH:
            return

        lead_is_abnormally_slow = lead_car.speed <= BRAKE_CHECK_LOW_SPEED_KMH
        chase_closing_fast = (chase_car.speed - lead_car.speed) >= BRAKE_CHECK_DELTA_KMH
        strong_impact = impact_speed >= BRAKE_CHECK_MIN_IMPACT
        self._last_collision_point_ts = now_ts
        if strong_impact and lead_is_abnormally_slow and chase_closing_fast:
            print(
                f"💥 [BATTLE] BRAKE CHECK PENALTY! Lead caused crash. "
                f"Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f}, Chase: {chase_car.speed:.1f})"
            )
            self._award_point(self.battle.chase_guid, reason="collision_brake_check")
        else:
            print(
                f"💥 [BATTLE] COLLISION Penalty! Chase hit Lead. "
                f"Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f}, Chase: {chase_car.speed:.1f})"
            )
            self._award_point(self.battle.lead_guid, reason="collision_penalty")

    def _process_logic(self):
        process_pair_logic(self)

    def _finalize_single_session_result(self, finish_gap_m, is_draw):
        finalize_single_session_result(self, finish_gap_m, is_draw)

    def _award_point(self, winner_guid, reason="outrun"):
        award_point(self, winner_guid, reason)

    def _abort_run_no_point(self, reason):
        # Silent abort: gap exceeded / false start / wrong order simply means
        # no battle started. We do not kick, restart the session, send anyone
        # to pits, or broadcast "BATTLE CANCELLED" in chat. We just return to
        # IDLE so the pair can re-arm naturally.
        print(f"\n⚠️ [BATTLE] Run aborted ({reason}). No point awarded.")
        self.state = "IDLE"

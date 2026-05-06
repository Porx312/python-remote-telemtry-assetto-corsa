import math
import time

from engines.battlesystem.models import CarState, TougeBattle
from engines.battlesystem.pair_manager import PairBattleManager
from engines.battlesystem.config import (
    PAIR_LOCK_MAX_DISTANCE_METERS,
    PAIR_LOCK_MIN_SPEED_KMH,
)


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

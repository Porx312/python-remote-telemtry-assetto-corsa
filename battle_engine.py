import math
import time

# ===================================================
# TEST MODE: True = allows 1 player for solo testing.
# Set to False in production to require 2 players.
# ===================================================
TEST_MODE_1_PLAYER = False

# Points needed to win the series (Best of N)
POINTS_TO_WIN = 2

# DNF detection: if a car is slower than this for this long during ACTIVE, opponent wins
DNF_SPEED_KMH = 5.0       # km/h threshold to consider a car "stalled"
DNF_TIME_SECONDS = 5.0    # seconds a car must be stalled before opponent earns the point


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

        # Callbacks set by main.py to        # Callback hooks
        # on_battle_start(car1_guid, car2_guid) -> battle_id
        # on_score_update(battle_id, p1_score, p2_score, winner_guid_or_None)
        self.on_battle_start = None
        self.on_score_update = None
        self.on_session_restart = None

        self.battle_id = None  # DB row id of the current active battle

        # DNF tracking: timestamps when each car last moved above stall speed
        self.car1_last_moving_time = 0.0
        self.car2_last_moving_time = 0.0

        # Auto-reset after FINISHED
        self.finished_time = 0.0
        self.FINISHED_COOLDOWN = 10.0  # Seconds before accepting a new battle

        # Battle config
        self.run_length_spline = 0.80   # Virtual finish line at 80% of spline
        self.judge_offset_spline = 0.03 # Draw tolerance
        self.overtake_margin_spline = 0.003 # Chase needs to be visibly ahead to pass
        self.active_start_time = 0.0

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
        if self.state in ["ARMED", "LAUNCHING", "ACTIVE"]:
            print(f"[BATTLE] Player {driver_guid} disconnected. Cancelling battle.")
            self._reset_to_idle()

    def _reset_to_idle(self, full_reset=False):
        self.state = "IDLE"
        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0
        self.car1_last_moving_time = 0.0
        self.car2_last_moving_time = 0.0
        self.finished_time = 0.0
        if full_reset:
            self.battle_id = None

    def handle_collision(self, car1_guid, car2_guid, impact_speed):
        """Called by main.py on CE_COLLISION_WITH_CAR."""
        if self.state != "ACTIVE" or TEST_MODE_1_PLAYER:
            return
            
        guids = {car1_guid, car2_guid}
        if self.battle.car1_guid not in guids or self.battle.car2_guid not in guids:
            return
            
        lead_car  = self.cars[self.battle.lead_guid]
        chase_car = self.cars[self.battle.chase_guid]
        
        # 1. Ignore "rubs" / light touches
        # Note: Assetto Corsa impact_speed might be quite low or in m/s, so we lower the threshold to 4.0
        if impact_speed < 4.0:
            print(f"⚠️ [BATTLE] Light rub ignored. Impact speed: {impact_speed:.2f} (Lead: {lead_car.speed:.1f} km/h, Chase: {chase_car.speed:.1f} km/h)")
            return
        
        # 2. Brake Checking Detection
        # If the LEAD car's speed is dangerously slow during the ACTIVE race (e.g. less than 40 km/h) 
        # or they are going significantly slower than the CHASE car (e.g. 15 km/h delta) when the crash happens,
        # we rule it a brake-check / blocking penalty against LEAD.
        if lead_car.speed < 40.0 or lead_car.speed < (chase_car.speed - 15.0):
            print(f"💥 [BATTLE] BRAKE CHECK PENALTY! Lead caused crash. Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f} km/h, Chase: {chase_car.speed:.1f} km/h)")
            self._award_point(self.battle.chase_guid, reason='collision_brake_check')
        else:
            # Standard rear-end collision, CHASE is at fault for not maintaining distance.
            print(f"💥 [BATTLE] COLLISION Penalty! Chase hit Lead. Impact: {impact_speed:.2f}. (Lead: {lead_car.speed:.1f} km/h, Chase: {chase_car.speed:.1f} km/h)")
            self._award_point(self.battle.lead_guid, reason='collision_penalty')

    def _process_logic(self):
        now = time.time()

        # Only consider cars that have sent telemetry in the last 5 seconds
        active_guids = [g for g, c in self.cars.items() if (now - c.last_update_time) < 5.0]

        min_players = 1 if TEST_MODE_1_PLAYER else 2

        if len(active_guids) < min_players:
            if self.state not in ["IDLE", "FINISHED"]:
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
                    self.state = "LAUNCHING"
                    self.launch_trigger_time = now
                    print(f"\n[BATTLE] ROLLING START DETECTED! Gap: {distance:.1f}m. Waiting for both cars to hit 40 km/h...")

        # ==========================
        # LAUNCHING: Confirm both cars launch
        # ==========================
        elif self.state == "LAUNCHING":
            # Persist battle start to DB physically as soon as rolling start begins
            if self.battle_id is None and self.on_battle_start:
                self.battle_id = self.on_battle_start(
                    self.battle.car1_guid, self.battle.car2_guid
                )

            if car1.speed > 40.0 and car2.speed > 40.0:
                # Before starting ACTIVE, check for false start (Jump Start)
                # In runs > 1, roles are predetermined. If Chase jumped and passed Lead, penalty!
                if self.battle.run_count >= 1:
                    pre_lead = self.battle.chase_guid
                    pre_chase = self.battle.lead_guid
                    # Calculate if pre_chase is ahead of pre_lead
                    c_lead = self.cars[pre_lead]
                    c_chase = self.cars[pre_chase]
                    jump_gap = (c_chase.spline - c_lead.spline) % 1.0
                    # If jump_gap < 0.5, chase is ahead of lead -> False Start
                    if jump_gap < 0.5 and jump_gap > 0.001:  # Added a small margin for side-by-side
                        print(f"🚨 [BATTLE] FALSE START! Chase ({pre_chase}) jumped the start and passed Lead ({pre_lead}).")
                        self.battle.lead_guid = pre_lead
                        self.battle.chase_guid = pre_chase
                        self._award_point(pre_lead, reason='false_start')
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

            elif now - self.launch_trigger_time > 3.0:
                print("[BATTLE] Timeout: opponent did not reach 40 km/h within 3s. Cancelling.")
                self._reset_to_idle()

        # ==========================
        # ACTIVE: Battle in progress
        # ==========================
        elif self.state == "ACTIVE":
            lead_car  = self.cars[self.battle.lead_guid]
            chase_car = self.cars[self.battle.chase_guid]

            # --- DNF detection: stalled car check ---
            if lead_car.speed >= DNF_SPEED_KMH:
                self.car1_last_moving_time = now
            if chase_car.speed >= DNF_SPEED_KMH:
                self.car2_last_moving_time = now

            # Initialize stall timers on first active frame
            if self.car1_last_moving_time == 0.0: self.car1_last_moving_time = now
            if self.car2_last_moving_time == 0.0: self.car2_last_moving_time = now

            lead_stalled  = (now - self.car1_last_moving_time) >= DNF_TIME_SECONDS
            chase_stalled = (now - self.car2_last_moving_time) >= DNF_TIME_SECONDS

            if lead_stalled and not TEST_MODE_1_PLAYER:
                print(f"🚨 [BATTLE] DNF: LEAD ({self.battle.lead_guid}) stalled for {DNF_TIME_SECONDS}s. Point for CHASE!")
                self._award_point(self.battle.chase_guid, reason='dnf_lead_stalled')
                return
            if chase_stalled and not TEST_MODE_1_PLAYER:
                print(f"🚨 [BATTLE] DNF: CHASE ({self.battle.chase_guid}) stalled for {DNF_TIME_SECONDS}s. Point for LEAD!")
                self._award_point(self.battle.lead_guid, reason='dnf_chase_stalled')
                return

            # If cars are too far apart during ACTIVE, reset
            if distance > 45.0 and not TEST_MODE_1_PLAYER:
                print(f"\n[BATTLE] Gap exceeded 45m while ACTIVE. Resetting to IDLE.")
                self._reset_to_idle()
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
        if winner_guid == self.battle.car1_guid:
            self.battle.car1_score += 1
        elif winner_guid == self.battle.car2_guid:
            self.battle.car2_score += 1

        # Log the point event
        self.battle.points_log.append({
            'scorer': winner_guid,
            'reason': reason,
            'ts': int(_time.time() * 1000)
        })

        print(f"🏅 [BATTLE] SCORE: P1={self.battle.car1_score} | P2={self.battle.car2_score} (reason: {reason})")

        # Best of N: first to POINTS_TO_WIN wins the series
        if self.battle.car1_score >= POINTS_TO_WIN or self.battle.car2_score >= POINTS_TO_WIN:
            self.state = "FINISHED"
            self.finished_time = 0.0  # Start cooldown timer
            self.battle.winner = self.battle.car1_guid if self.battle.car1_score >= POINTS_TO_WIN else self.battle.car2_guid
            print(f"🏆 [BATTLE] SERIES OVER! WINNER: {self.battle.winner}")
            self._end_run() # Immediately stop the active run logic
            self.state = "FINISHED" # override IDLE with FINISHED
            # Save final result
            if self.on_score_update:
                self.on_score_update(self.battle_id, self.battle.car1_score, self.battle.car2_score, self.battle.winner, self.battle.points_log)
        else:
            # Save live score after each intermediate point
            if self.on_score_update:
                self.on_score_update(self.battle_id, self.battle.car1_score, self.battle.car2_score, None, self.battle.points_log)
            self._end_run()


    def _end_run(self):
        """Returns to IDLE and resets driven distances for the next run."""
        self.state = "IDLE"
        self.condition_start_time = 0.0
        self.launch_trigger_time = 0.0
        for c in self.cars.values():
            c.driven_spline = 0.0
            
        print("🔄 [BATTLE] Run complete. Restarting session to return players to pits...")
        if self.on_session_restart:
            self.on_session_restart()

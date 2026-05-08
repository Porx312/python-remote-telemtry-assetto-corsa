import time


class TimeAttackEngine:
    """
    Lightweight per-driver state machine used during Time Attack / Endurance
    sessions. With the migration to the Redis -> Convex pipeline, no more
    DB-backed event metadata exists, so most constraint flags default to off
    unless future per-server runtime config injects them.
    """

    def __init__(self, send_chat_callback, send_admin_command_callback, server_state_ref):
        self.send_chat = send_chat_callback
        self.send_admin_command = send_admin_command_callback
        self.server_state = server_state_ref

    def _reset_driver_lap_state(self, driver):
        driver.lap_start_time = int(time.time() * 1000)
        driver.had_collision = False
        driver.restarted_lap = False
        driver.was_idle = False
        driver.lap_notified_fail = False
        driver.collision_notified = False
        driver.idle_notified = False
        driver.has_left_pits = False

    def check_idle(self, driver, speed_ms, now_ms, meta):
        """Called every CAR_UPDATE to flag drivers stopped on track."""
        if speed_ms < 0.5:
            if getattr(driver, 'last_pos_time', 0) == 0:
                driver.last_pos_time = now_ms
            elif (now_ms - driver.last_pos_time) > 5000:
                driver.was_idle = True
                if (
                    meta.get("detectIdle", False)
                    and getattr(driver, "has_left_pits", False)
                    and not getattr(driver, "idle_notified", False)
                ):
                    driver.idle_notified = True
                    driver.failed_laps = getattr(driver, 'failed_laps', 0) + 1
                    driver.lap_notified_fail = True
                    max_fails = meta.get("maxFails", "?")
                    self.send_chat(
                        driver.car_id,
                        f"[EVENT] Lap FAILED: Stopped on track (>5s) ({driver.failed_laps}/{max_fails} fails)",
                    )
                    self.send_admin_command(f"/pit {driver.car_id}")
        else:
            driver.last_pos_time = 0
            if speed_ms > 5.0:
                driver.has_left_pits = True
                driver.idle_notified = False
                driver.collision_notified = False

    def check_collision(self, driver, meta):
        """Called on CLIENT_EVENT collision packets."""
        driver.had_collision = True
        if (
            meta.get("enableCollisions", False)
            and getattr(driver, "has_left_pits", False)
            and not getattr(driver, "collision_notified", False)
        ):
            driver.collision_notified = True
            driver.failed_laps = getattr(driver, 'failed_laps', 0) + 1
            driver.lap_notified_fail = True
            max_fails = meta.get("maxFails", "?")
            self.send_chat(
                driver.car_id,
                f"[EVENT] Lap FAILED: Collision ({driver.failed_laps}/{max_fails} fails)",
            )
            self.send_admin_command(f"/pit {driver.car_id}")
            driver.idle_notified = True

    def evaluate_lap(self, driver, ac_lap_time, cuts, meta):
        """
        Called on LAP_COMPLETED to summarize per-lap constraints.
        Returns (is_valid, fail_reason).
        """
        is_valid = (cuts == 0)
        fail_reason = ""

        if meta.get("enableCollisions", False) and getattr(driver, "had_collision", False):
            is_valid = False
            fail_reason = "Collision"

        if meta.get("detectIdle", False) and getattr(driver, "was_idle", False) and driver.lap_count > 1:
            is_valid = False
            fail_reason = "Stopped on track (>5s)"

        if cuts > 0:
            is_valid = False
            fail_reason = "Track Cut / Teleport"

        was_notified = getattr(driver, 'lap_notified_fail', False)
        self._reset_driver_lap_state(driver)

        if not is_valid and not was_notified and meta:
            driver.failed_laps = getattr(driver, 'failed_laps', 0) + 1
            max_fails = meta.get("maxFails", "?")
            fail_reason_display = fail_reason if fail_reason else "Track Cut"
            self.send_chat(
                driver.car_id,
                f"[EVENT] Lap FAILED: {fail_reason_display} ({driver.failed_laps}/{max_fails} fails)",
            )
            self.send_admin_command(f"/pit {driver.car_id}")

        return is_valid, fail_reason

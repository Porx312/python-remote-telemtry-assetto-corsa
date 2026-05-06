import time


class CarState:
    """Tracks the real-time and accumulated state of a single driver."""

    def __init__(self, guid):
        self.guid = guid
        self.spline = 0.0
        self.speed = 0.0
        self.pos = (0.0, 0.0, 0.0)
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
        self.points_log = []

    def get_opponent(self, guid):
        return self.car2_guid if guid == self.car1_guid else self.car1_guid

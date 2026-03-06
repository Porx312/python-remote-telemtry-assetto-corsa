"""
event_dispatcher.py
====================
Handles building and sending webhook payloads based on the active event type
registered in the `server_events` database table.

Supported event types:
  - endurance_progress
  - time_attack         (coming soon)
  - drift_score         (coming soon)
  - race_finished       (coming soon)

To add a new event type:
  1. Add an `elif event_type == "your_type":` branch in `build_payload()`
  2. Add a `dispatch_event()` call from `main.py`
"""

import os
import threading
import requests
from database import get_active_server_event

ACAPI_KEY      = os.getenv("API_KEY", "")
WEBHOOK_SECRET = os.getenv("EVENT_WEBHOOK_SECRET", "default_secret")


# ─────────────────────────────────────────────────────────────
# Payload builders per event type
# ─────────────────────────────────────────────────────────────

def build_endurance_payload(event_id, driver, lap_time_ms, is_still_going=True):
    return {
        "eventId":   event_id,
        "eventType": "endurance",
        "data": {
            "steamId":       driver.guid,
            "isStillGoing":  is_still_going,
            "completedLaps": driver.lap_count,
            "bestLapMs":     driver.best_lap,
            "lastLapMs":     lap_time_ms,
            "failedLaps":    getattr(driver, "failed_laps", 0),
        }
    }


def build_time_attack_payload(event_id, driver, lap_time_ms):
    return {
        "eventId":   event_id,
        "eventType": "time_attack",
        "data": {
            "player":       driver.name,
            "steamId":      driver.guid,
            "bestLapMs":    driver.best_lap,
            "lastLapMs":    lap_time_ms,
            "totalLaps":    driver.lap_count,
            "car":          driver.model,
        }
    }


def build_drift_payload(event_id, driver, score):
    return {
        "eventId":   event_id,
        "eventType": "drift_score",
        "data": {
            "player":   driver.name,
            "steamId":  driver.guid,
            "score":    score,
            "car":      driver.model,
        }
    }


# ─────────────────────────────────────────────────────────────
# Core dispatcher (non-blocking)
# ─────────────────────────────────────────────────────────────

def dispatch_event(server_state, driver, lap_time_ms=None, drift_score=None, is_finished=False):
    """
    Looks up the active event for this server, builds the correct payload and
    fires an HTTP POST to the registered webhook URL in a background thread.
    """

    def _send():
        try:
            # Try session name first ("ProjectD"), then fall back to .ini config name ("Events Server")
            event = get_active_server_event(server_state.server_name)
            if not event:
                event = get_active_server_event(getattr(server_state, 'config_server_name', server_state.server_name))
            if not event or not event.get("webhook_url"):
                return  # No active event registered for this server

            webhook_url    = event["webhook_url"]
            event_type     = event.get("event_type", "unknown")
            meta           = event.get("metadata", {})
            event_id       = meta.get("eventId", None)

            # Route to correct payload builder
            if event_type in ("endurance", "endurance_progress"):
                payload = build_endurance_payload(event_id, driver, lap_time_ms or 0, is_still_going=not is_finished)

            elif event_type == "time_attack":
                payload = build_time_attack_payload(event_id, driver, lap_time_ms or 0)

            elif event_type == "drift_score":
                payload = build_drift_payload(event_id, driver, drift_score or 0)

            else:
                print(f"⚠️  [{server_state.port}] [EVENTS] Unknown event type: '{event_type}'. Skipping dispatch.")
                return

            resp = requests.post(
                webhook_url,
                json=payload,
                headers={
                    "Content-Type":     "application/json",
                    "x-webhook-secret": WEBHOOK_SECRET,
                },
                timeout=10
            )
            print(
                f"📡 [{server_state.port}] [EVENT:{event_type}] → HTTP {resp.status_code} "
                f"| {driver.name} lap #{driver.lap_count} ({lap_time_ms}ms)"
            )

        except Exception as e:
            print(f"❌ [{server_state.port}] [EVENTS] Error dispatching: {e}")

    threading.Thread(target=_send, daemon=True).start()

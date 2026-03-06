"""
simulate_endurance.py  (Events Server version)
================================================
Simulates an Assetto Corsa player completing laps in an active event.
Auto-discovers the server name from the DB — no hardcoding needed.

Usage:
    python simulate_endurance.py
"""

import requests
import time
import random
import sys
import os
from dotenv import load_dotenv
from database import get_active_server_event, list_all_server_events
from event_dispatcher import build_endurance_payload, build_time_attack_payload

load_dotenv()

# ─── CONFIGURE FAKE DRIVER ──────────────────────────────────
FAKE_DRIVER   = "TestPiloto"
FAKE_STEAM_ID = "76561199230780195"   # Steam ID real de pruebas
FAKE_MODEL    = "bmw_m3_e30"
NUM_LAPS        = 5
LAP_INTERVAL    = 1         # seconds between simulated laps
BASE_LAP_MS     = 270_000   # ~4:30 base lap time in ms
INJECT_FAILED_LAP = True    # Envía una vuelta fallida antes de las válidas
# ────────────────────────────────────────────────────────────

API_KEY        = os.getenv("API_KEY", "")
WEBHOOK_SECRET = os.getenv("EVENT_WEBHOOK_SECRET", "default_secret")


class FakeDriver:
    def __init__(self):
        self.name        = FAKE_DRIVER
        self.guid        = FAKE_STEAM_ID
        self.model       = FAKE_MODEL
        self.best_lap    = 0
        self.last_lap    = 0
        self.lap_count   = 0
        self.failed_laps = 0


def pick_server():
    """Auto-detect or let the user pick a server from the registered events."""
    all_events = list_all_server_events()

    if not all_events:
        print("❌ No events registered in the database.")
        print("   POST to /ac-server/webhook first with serverName, webhookUrl and metadata.")
        sys.exit(1)

    if len(all_events) == 1:
        row = all_events[0]
        print(f"✅ Auto-selected only registered event → '{row['server_name']}' ({row['event_type']})")
        return row['server_name']

    print("\n📋 Registered server events in DB:")
    print(f"  {'#':<4} {'Server Name':<30} {'Event Type':<25} {'ID'}")
    print("  " + "─" * 75)
    for i, row in enumerate(all_events):
        print(f"  {i:<4} {row['server_name']:<30} {row['event_type']:<25} {row['id']}")

    while True:
        try:
            choice = input(f"\n  Select # [0-{len(all_events)-1}]: ").strip()
            idx = int(choice)
            if 0 <= idx < len(all_events):
                return all_events[idx]['server_name']
        except (ValueError, KeyboardInterrupt):
            pass
        print("  ❌ Invalid choice, try again.")


def main():
    server_name = pick_server()

    print(f"\n🔍 Fetching active event for server: '{server_name}'...")
    event = get_active_server_event(server_name)

    if not event or not event.get("webhook_url"):
        print(f"❌ No active event found for server '{server_name}'.")
        return

    webhook_url    = event["webhook_url"]
    event_type     = event.get("event_type", "unknown")
    event_id       = event.get("metadata", {}).get("eventId", "UNKNOWN")

    print(f"✅ Event found!")
    print(f"   Type       : {event_type}")
    print(f"   Event ID   : {event_id}")
    print(f"   Webhook URL: {webhook_url}")
    print(f"   Simulating {NUM_LAPS} laps for '{FAKE_DRIVER}'...\n")

    driver = FakeDriver()

    # ── Vuelta fallida opcional ───────────────────────────────
    if INJECT_FAILED_LAP:
        driver.failed_laps = 1   # Incrementa fallos, NO completedLaps ni bestLap
        failed_payload = build_endurance_payload(
            event_id, driver,
            lap_time_ms=BASE_LAP_MS + 5_000,
            is_still_going=True
        )
        try:
            resp = requests.post(
                webhook_url,
                json=failed_payload,
                headers={"Content-Type": "application/json", "x-webhook-secret": WEBHOOK_SECRET},
                timeout=10,
            )
            icon = "✅" if resp.status_code < 300 else "⚠️"
            print(f"  {icon} [FAILED LAP] failedLaps=1 | completedLaps=0 | bestLapMs=0 | HTTP {resp.status_code}")
        except Exception as e:
            print(f"  ❌ [FAILED LAP] error: {e}")
        time.sleep(LAP_INTERVAL)

    for lap_num in range(1, NUM_LAPS + 1):
        this_lap = BASE_LAP_MS + random.randint(-8_000, 12_000)
        if driver.best_lap == 0 or this_lap < driver.best_lap:
            driver.best_lap = this_lap
        driver.last_lap  = this_lap
        driver.lap_count = lap_num
        is_last          = (lap_num == NUM_LAPS)

        if event_type in ("endurance", "endurance_progress"):
            payload = build_endurance_payload(event_id, driver, this_lap, is_still_going=not is_last)
        elif event_type == "time_attack":
            payload = build_time_attack_payload(event_id, driver, this_lap)
        else:
            print(f"⚠️  Unknown event type '{event_type}'. Cannot build payload.")
            return

        try:
            resp = requests.post(
                webhook_url,
                json=payload,
                headers={
                    "Content-Type":     "application/json",
                    "x-webhook-secret": WEBHOOK_SECRET,
                },
                timeout=10
            )
            icon = "✅" if resp.status_code < 300 else "⚠️"
            print(f"  {icon} Lap {lap_num:2d}/{NUM_LAPS} | {this_lap}ms | Best: {driver.best_lap}ms | HTTP {resp.status_code}")
        except Exception as e:
            print(f"  ❌ Lap {lap_num:2d} error: {e}")

        if not is_last:
            time.sleep(LAP_INTERVAL)

    print(f"\n🏆 Simulation done. '{FAKE_DRIVER}' completed {NUM_LAPS} laps.")


if __name__ == "__main__":
    main()

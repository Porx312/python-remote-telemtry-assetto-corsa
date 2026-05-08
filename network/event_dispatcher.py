"""
event_dispatcher.py
====================
Publishes runtime telemetry / battle events to the Redis Stream consumed by
the `ac-data` bridge, which forwards them to Convex.

This module is Redis-only by design. All HTTP webhook fallbacks have been
removed: every public function in this file ends up calling
`_publish_redis_event(...)`.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid

from dotenv import load_dotenv

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover - optional dependency guard
    redis = None  # type: ignore[assignment]


load_dotenv()

AC_INSTANCE_ID = os.getenv("AC_INSTANCE_ID", "unknown-instance")

REDIS_HOST = os.getenv("REDIS_HOST", "")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_SSL = os.getenv("REDIS_SSL", "false").strip().lower() == "true"
REDIS_STREAM_KEY = os.getenv("REDIS_STREAM_KEY", "ac:events")
REDIS_STREAM_MAXLEN = int(os.getenv("REDIS_STREAM_MAXLEN", "20000"))
REDIS_SCHEMA_VERSION = os.getenv("REDIS_SCHEMA_VERSION", "1")

_redis_client = None
_redis_lock = threading.Lock()


def _get_redis_client():
    global _redis_client
    if redis is None:
        raise RuntimeError(
            "Redis transport selected but 'redis' package is not installed. "
            "Run: pip install redis (or pip install -r requirements.txt)"
        )
    if _redis_client is not None:
        return _redis_client
    with _redis_lock:
        if _redis_client is not None:
            return _redis_client
        if not REDIS_HOST:
            raise RuntimeError("REDIS_HOST is not configured")
        _redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            username=REDIS_USERNAME or None,
            password=REDIS_PASSWORD or None,
            db=REDIS_DB,
            ssl=REDIS_SSL,
        )
    return _redis_client


def _publish_redis_event(event_type: str, server_name: str, data: dict) -> None:
    client = _get_redis_client()
    event_id = str(uuid.uuid4())
    envelope = {
        "eventId": event_id,
        "schemaVersion": REDIS_SCHEMA_VERSION,
        "event": event_type,
        "serverName": server_name,
        "instanceId": AC_INSTANCE_ID,
        "ts": int(time.time() * 1000),
        "data": data,
    }
    fields = {
        "event": event_type,
        "eventId": event_id,
        "schemaVersion": REDIS_SCHEMA_VERSION,
        "instanceId": AC_INSTANCE_ID,
        "serverName": str(server_name or ""),
        "ts": str(envelope["ts"]),
        "payload": json.dumps(envelope, separators=(",", ":"), ensure_ascii=False),
    }
    client.xadd(REDIS_STREAM_KEY, fields, maxlen=REDIS_STREAM_MAXLEN, approximate=True)


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────


def send_server_event(event_type: str, server_name: str, data: dict) -> None:
    """
    Publish a generic server event (`player_join`, `player_leave`,
    `lap_completed`, `server_status`, `server_config_applied`, ...) into the
    Redis Stream. Non-blocking: errors are logged but never raised.
    """

    def _send() -> None:
        try:
            _publish_redis_event(event_type, server_name, data)
        except Exception as exc:  # noqa: BLE001 - log and continue
            print(f"❌ [REDIS] dispatch '{event_type}' failed: {exc}")

    threading.Thread(target=_send, daemon=True).start()


def dispatch_battle_webhook(
    server_state,
    battle_config: dict,
    p1_score: int,
    p2_score: int,
    winner_guid: str | None,
    points_log,
) -> None:
    """
    Publish a `battle_update` event for live touge battles. When `winner_guid`
    is set, an additional `battle_finished` event is published with the same
    payload so consumers can react to the finalization without ambiguity.
    """

    def _send() -> None:
        try:
            meta = battle_config.get("metadata", {}) or {}
            payload = {
                "battleId": battle_config.get("battle_id"),
                "player1SteamId": battle_config.get("player1_steam_id"),
                "player2SteamId": battle_config.get("player2_steam_id"),
                "player1Score": p1_score,
                "player2Score": p2_score,
                "player1Car": meta.get("player1Car", ""),
                "player2Car": meta.get("player2Car", ""),
                "player1Name": meta.get("player1Name", ""),
                "player2Name": meta.get("player2Name", ""),
                "pointsLog": points_log or [],
                "status": "finished" if winner_guid else "active",
                "serverName": getattr(server_state, "server_name", ""),
                "track": meta.get("track", ""),
                "trackConfig": meta.get("trackConfig", ""),
            }
            if winner_guid:
                payload["winnerSteamId"] = winner_guid

            server_name = getattr(server_state, "server_name", "")
            _publish_redis_event("battle_update", server_name, payload)
            print(
                "📨 [REDIS] battle_update published | "
                f"battleId={payload.get('battleId')} status={payload.get('status')} "
                f"score={payload.get('player1Score')}-{payload.get('player2Score')}"
            )
            if winner_guid:
                _publish_redis_event("battle_finished", server_name, payload)
                print(
                    "📨 [REDIS] battle_finished published | "
                    f"battleId={payload.get('battleId')} winner={winner_guid}"
                )
        except Exception as exc:  # noqa: BLE001 - log and continue
            print(f"❌ [REDIS] battle dispatch failed: {exc}")

    threading.Thread(target=_send, daemon=True).start()

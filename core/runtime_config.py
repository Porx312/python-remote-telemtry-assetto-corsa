"""
runtime_config.py
==================
In-memory cache of per-server runtime configuration sourced from the Redis
config stream (`server_config_snapshot`). Replaces the legacy SQL-based
`get_server_mode_for_instance` / `get_active_server_event` lookups.

The Redis snapshot publisher (Node bridge) sends one envelope per change of
config in Convex; our consumer in `core/redis_config_sync.py` calls
`set_server_modes(...)` so the rest of the script can resolve server modes
without touching any database.
"""

from __future__ import annotations

import threading
from typing import Iterable, Optional


_lock = threading.Lock()
_modes: dict[str, str] = {}


def _normalize_mode(value: object) -> str:
    text = (str(value) if value is not None else "").strip().lower()
    text = text.replace("_", "-")
    if text in {"battle", "time-attack", "event"}:
        return text
    return ""


def set_server_modes(rows: Iterable[dict]) -> None:
    """
    Replace the server mode mapping with values derived from a snapshot row list.

    Each row may contain `serverName` (folder slug, e.g. "server-2"),
    `displayName` (the AC SERVER_NAME), and `type` (battle | time-attack | event).
    All keys are lowercased to make lookups case-insensitive.
    """
    new_map: dict[str, str] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        mode = _normalize_mode(row.get("type"))
        if not mode:
            continue
        for key in ("serverName", "displayName"):
            value = row.get(key)
            if not value:
                continue
            new_map[str(value).strip().lower()] = mode
    with _lock:
        _modes.clear()
        _modes.update(new_map)


def get_mode_for_state(state) -> Optional[str]:
    """
    Resolve the active mode for a `ServerState`. Tries (in order) folder id,
    .ini server name, and the runtime server name reported by Assetto Corsa.
    Returns ``None`` when the snapshot has not yet been received.
    """
    candidates = (
        getattr(state, "server_folder_id", "") or "",
        getattr(state, "config_server_name", "") or "",
        getattr(state, "server_name", "") or "",
    )
    with _lock:
        if not _modes:
            return None
        for candidate in candidates:
            key = str(candidate).strip().lower()
            if key and key in _modes:
                return _modes[key]
    return None


def has_data() -> bool:
    """True when at least one snapshot has populated the cache."""
    with _lock:
        return bool(_modes)


def snapshot() -> dict[str, str]:
    """Return a shallow copy of the current mode map (for diagnostics)."""
    with _lock:
        return dict(_modes)

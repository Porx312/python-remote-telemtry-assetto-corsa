import json
import os
import re
import threading
import time
from typing import Any, Dict, Optional

from dotenv import load_dotenv

try:
    import redis
except Exception:
    redis = None

from core import runtime_config
from network.event_dispatcher import send_server_event


load_dotenv()

AC_INSTANCE_ID = os.getenv("AC_INSTANCE_ID", "default").strip() or "default"
REDIS_HOST = os.getenv("REDIS_HOST", "").strip()
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "").strip() or None
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "").strip() or None
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_SSL = os.getenv("REDIS_SSL", "false").strip().lower() == "true"
REDIS_STREAM_KEY = os.getenv("REDIS_STREAM_KEY", "ac:events").strip()
REDIS_CONFIG_STREAM_KEY = os.getenv("REDIS_CONFIG_STREAM_KEY", "ac:config").strip() or REDIS_STREAM_KEY

REDIS_CONFIG_CONSUMER_ENABLED = (
    os.getenv("REDIS_CONFIG_CONSUMER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
)
REDIS_CONFIG_CONSUMER_GROUP = os.getenv("REDIS_CONFIG_CONSUMER_GROUP", "ac-config-consumers").strip()
REDIS_CONFIG_CONSUMER_NAME = os.getenv("REDIS_CONFIG_CONSUMER_NAME", f"py-{AC_INSTANCE_ID}").strip()

_VERSIONS_FILE = os.path.join(os.getcwd(), "redis_applied_config_versions.json")
_versions_lock = threading.Lock()


def _load_versions() -> Dict[str, str]:
    try:
        if os.path.exists(_VERSIONS_FILE):
            with open(_VERSIONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_versions(data: Dict[str, str]) -> None:
    try:
        with open(_VERSIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ [CONFIG-SYNC] Could not save versions file: {e}")


def _replace_or_append(content: str, key: str, value: str) -> str:
    line = f"{key}={value}"
    pattern = rf"(?m)^{re.escape(key)}=.*$"
    if re.search(pattern, content):
        return re.sub(pattern, line, content)
    return content.rstrip() + f"\n{line}\n"


def _write_server_cfg(cfg_path: str, cfg: Dict[str, Any]) -> list[str]:
    with open(cfg_path, "rb") as f:
        raw = f.read()
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        content = raw.decode("utf-16le", errors="ignore")

    changed: list[str] = []
    mappings = [
        ("NAME", cfg.get("displayName")),
        ("PASSWORD", cfg.get("password")),
        ("TRACK", cfg.get("track")),
        ("CONFIG_TRACK", cfg.get("trackConfig")),
        ("MAX_CLIENTS", cfg.get("maxClients")),
    ]
    for key, value in mappings:
        if value is None:
            continue
        next_content = _replace_or_append(content, key, str(value))
        if next_content != content:
            changed.append(key)
            content = next_content

    entries = cfg.get("entries")
    if isinstance(entries, list):
        car_models = []
        for entry in entries:
            model = str((entry or {}).get("model") or "").strip()
            if not model:
                continue
            count = int((entry or {}).get("count") or 1)
            for _ in range(max(1, count)):
                car_models.append(model)
        if car_models:
            cars_value = ";".join(sorted(set(car_models)))
            next_content = _replace_or_append(content, "CARS", cars_value)
            if next_content != content:
                changed.append("CARS")
                content = next_content

    with open(cfg_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
    return changed


def _write_entry_list(cfg_path: str, cfg: Dict[str, Any]) -> bool:
    entries = cfg.get("entries")
    if not isinstance(entries, list):
        return False
    cfg_dir = os.path.dirname(cfg_path)
    entry_list_path = os.path.join(cfg_dir, "entry_list.ini")

    blocks = []
    idx = 0
    for entry in entries:
        model = str((entry or {}).get("model") or "").strip()
        if not model:
            continue
        skin = str((entry or {}).get("skin") or "0_default")
        count = int((entry or {}).get("count") or 1)
        for _ in range(max(1, count)):
            blocks.append(
                "\n".join(
                    [
                        f"[CAR_{idx}]",
                        f"MODEL={model}",
                        f"SKIN={skin}",
                        "SPECTATOR_MODE=0",
                        "DRIVERNAME=",
                        "TEAM=",
                        "GUID=",
                        "BALLAST=0",
                        "RESTRICTOR=0",
                        "",
                    ]
                )
            )
            idx += 1

    with open(entry_list_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(blocks).rstrip() + "\n")
    return True


def _find_state_by_server_name(
    servers: Dict[int, Any], server_name: str, display_name: str = ""
) -> Optional[Any]:
    """
    Resolve a `ServerState` from a snapshot row. The Convex `serverName` is the
    folder slug (e.g. ``server-2``) while `displayName` matches the AC
    SERVER_NAME, so we try multiple state attributes to find a hit.
    """
    targets = []
    for value in (server_name, display_name):
        text = (value or "").strip().lower()
        if text and text not in targets:
            targets.append(text)
    if not targets:
        return None
    for state in servers.values():
        candidates = [
            (getattr(state, "server_folder_id", "") or "").strip().lower(),
            (getattr(state, "config_server_name", "") or "").strip().lower(),
            (getattr(state, "server_name", "") or "").strip().lower(),
        ]
        for target in targets:
            if target in candidates:
                return state
    return None


def _apply_snapshot(servers: Dict[int, Any], payload: Dict[str, Any]) -> tuple[int, int]:
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        return 0, 0
    instance_id = str(data.get("instanceId") or payload.get("instanceId") or "")
    version = str(data.get("version") or "")
    if instance_id != AC_INSTANCE_ID or not version:
        return 0, 0

    rows = data.get("servers") or []
    if not isinstance(rows, list):
        rows = []

    runtime_config.set_server_modes(rows)

    with _versions_lock:
        versions = _load_versions()
        if versions.get(instance_id) == version:
            return 0, 0

    applied = 0
    errors = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        state = _find_state_by_server_name(
            servers,
            str(row.get("serverName") or ""),
            str(row.get("displayName") or ""),
        )
        if not state or not getattr(state, "cfg_path", None):
            continue
        try:
            changed = _write_server_cfg(state.cfg_path, row)
            entry_written = _write_entry_list(state.cfg_path, row)
            applied += 1
            send_server_event(
                "server_config_applied",
                getattr(state, "config_server_name", getattr(state, "server_name", "unknown")),
                {
                    "instanceId": AC_INSTANCE_ID,
                    "version": version,
                    "serverName": row.get("serverName"),
                    "updatedKeys": changed,
                    "entryListUpdated": entry_written,
                    "ok": True,
                },
            )
        except Exception as e:
            errors += 1
            send_server_event(
                "server_config_applied",
                getattr(state, "config_server_name", getattr(state, "server_name", "unknown")),
                {
                    "instanceId": AC_INSTANCE_ID,
                    "version": version,
                    "serverName": row.get("serverName"),
                    "ok": False,
                    "error": str(e),
                },
            )

    if applied > 0 and errors == 0:
        with _versions_lock:
            versions = _load_versions()
            versions[instance_id] = version
            _save_versions(versions)
    return applied, errors


def start_redis_config_consumer(servers: Dict[int, Any]) -> None:
    if not REDIS_CONFIG_CONSUMER_ENABLED:
        print("[CONFIG-SYNC] disabled by REDIS_CONFIG_CONSUMER_ENABLED")
        return
    if not REDIS_HOST:
        print("[CONFIG-SYNC] REDIS_HOST missing, consumer disabled")
        return
    if redis is None:
        print("[CONFIG-SYNC] redis package missing. Run: pip install redis")
        return

    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        ssl=REDIS_SSL,
    )
    try:
        client.xgroup_create(REDIS_CONFIG_STREAM_KEY, REDIS_CONFIG_CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass

    print(
        f"[CONFIG-SYNC] listening stream={REDIS_CONFIG_STREAM_KEY} "
        f"group={REDIS_CONFIG_CONSUMER_GROUP} consumer={REDIS_CONFIG_CONSUMER_NAME} instance={AC_INSTANCE_ID}"
    )
    while True:
        try:
            res = client.xreadgroup(
                REDIS_CONFIG_CONSUMER_GROUP,
                REDIS_CONFIG_CONSUMER_NAME,
                {REDIS_CONFIG_STREAM_KEY: ">"},
                count=25,
                block=5000,
            )
            if not res:
                continue
            for _stream, messages in res:
                for msg_id, fields in messages:
                    try:
                        raw_payload = fields.get("payload")
                        if not raw_payload:
                            client.xack(REDIS_CONFIG_STREAM_KEY, REDIS_CONFIG_CONSUMER_GROUP, msg_id)
                            continue
                        payload = json.loads(raw_payload)
                        if payload.get("event") != "server_config_snapshot":
                            client.xack(REDIS_CONFIG_STREAM_KEY, REDIS_CONFIG_CONSUMER_GROUP, msg_id)
                            continue
                        applied, errors = _apply_snapshot(servers, payload)
                        if applied or errors:
                            print(
                                f"[CONFIG-SYNC] processed snapshot version="
                                f"{(payload.get('data') or {}).get('version')} applied={applied} errors={errors}"
                            )
                        client.xack(REDIS_CONFIG_STREAM_KEY, REDIS_CONFIG_CONSUMER_GROUP, msg_id)
                    except Exception as e:
                        print(f"[CONFIG-SYNC] message error: {e}")
        except Exception as e:
            print(f"[CONFIG-SYNC] loop error: {e}")
            time.sleep(1)

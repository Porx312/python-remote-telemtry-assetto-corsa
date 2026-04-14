import json
import os
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import psycopg2
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import Json, RealDictCursor

load_dotenv()

# Supabase Postgres: Project Settings → Database → URI (usa sslmode=require)
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL", "")
AC_INSTANCE_ID = (os.getenv("AC_INSTANCE_ID") or os.getenv("VPS_ID") or "").strip()

if os.getenv("USE_TEST_DB", "false").lower() == "true":
    DATABASE_URL = os.getenv("TEST_DATABASE_URL") or DATABASE_URL


def _normalize_database_url(url: str) -> str:
    """
    Ajusta parámetros libpq para Supabase / Postgres remoto:
    - sslmode (require por defecto; sobrescribible con DATABASE_SSLMODE)
    - keepalives: reduce cortes "SSL connection has been closed unexpectedly" por idle
    - connect_timeout: evita bloqueos largos si la red cae
    """
    if not url:
        return url
    parsed = urlparse(url)
    if parsed.scheme not in ("postgresql", "postgres"):
        return url
    q = parse_qs(parsed.query, keep_blank_values=True)
    keys_lower = {k.lower() for k in q.keys()}

    sslmode_env = (
        os.getenv("DATABASE_SSLMODE") or os.getenv("SUPABASE_DB_SSLMODE") or ""
    ).strip().lower()
    if sslmode_env in (
        "disable",
        "allow",
        "prefer",
        "require",
        "verify-ca",
        "verify-full",
    ):
        q["sslmode"] = [sslmode_env]
    elif "sslmode" not in keys_lower and "gssencmode" not in keys_lower:
        q["sslmode"] = ["require"]

    defaults = {
        "connect_timeout": os.getenv("DATABASE_CONNECT_TIMEOUT", "15"),
        "keepalives": "1",
        "keepalives_idle": os.getenv("DATABASE_KEEPALIVES_IDLE", "30"),
        "keepalives_interval": os.getenv("DATABASE_KEEPALIVES_INTERVAL", "10"),
        "keepalives_count": os.getenv("DATABASE_KEEPALIVES_COUNT", "3"),
    }
    for param, val in defaults.items():
        if param not in keys_lower:
            q[param] = [val]

    flat = []
    for k, vals in q.items():
        for v in vals:
            flat.append((k, v))
    new_query = urlencode(flat)
    return urlunparse(parsed._replace(query=new_query))


DATABASE_URL = _normalize_database_url(DATABASE_URL)

if DATABASE_URL:
    safe = urlparse(DATABASE_URL)
    host = safe.hostname or "(no host)"
    print(f"✅ Database: PostgreSQL (Supabase) → {host}")
else:
    print("❌ DATABASE_URL o SUPABASE_DB_URL no está definido en el entorno.")

db_pool = None
try:
    if DATABASE_URL:
        db_pool = pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
except Exception as err:
    print(f"[DB] Error al crear el pool de conexiones: {err}")
    db_pool = None


class _PooledConn:
    """Devuelve la conexión al pool al llamar a close() (comportamiento tipo mysql-connector)."""

    __slots__ = ("_raw",)

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def close(self):
        if not db_pool or not self._raw:
            self._raw = None
            return
        raw = self._raw
        self._raw = None
        try:
            db_pool.putconn(raw)
        except Exception:
            try:
                db_pool.putconn(raw, close=True)
            except Exception:
                try:
                    raw.close()
                except Exception:
                    pass


class _DirectConn:
    """Conexión fuera del pool (fallback si getconn falla o no hay pool)."""

    __slots__ = ("_raw",)

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def close(self):
        if self._raw:
            try:
                self._raw.close()
            except Exception:
                pass
            self._raw = None


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL / SUPABASE_DB_URL no configurado")
    if db_pool:
        try:
            return _PooledConn(db_pool.getconn())
        except Exception as e:
            print(f"[DB] pool getconn failed ({e}); using direct connection")
    return _DirectConn(psycopg2.connect(DATABASE_URL))


# Cache: si `id` es IDENTITY en Supabase, hace falta OVERRIDING SYSTEM VALUE al insertar un id explícito.
_lap_id_is_identity: Optional[bool] = None
_table_has_instance_id_cache = {}


def _lap_id_needs_overriding(cursor) -> bool:
    global _lap_id_is_identity
    if _lap_id_is_identity is not None:
        return _lap_id_is_identity
    try:
        cursor.execute(
            """
            SELECT COALESCE(
                (SELECT c.is_identity = 'YES'
                 FROM information_schema.columns c
                 WHERE c.table_schema = 'public'
                   AND c.table_name = 'lap_records'
                   AND c.column_name = 'id'),
                false
            )
            """
        )
        row = cursor.fetchone()
        _lap_id_is_identity = bool(row and row[0])
    except Exception:
        _lap_id_is_identity = False
    return _lap_id_is_identity


def _next_lap_record_id(cursor) -> int:
    """Drizzle define `id` sin SERIAL: generamos el siguiente entero (misma idea que AUTO_INCREMENT)."""
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM lap_records")
    row = cursor.fetchone()
    return int(row[0]) + 1


def _table_has_instance_id(cursor, table_name: str) -> bool:
    cached = _table_has_instance_id_cache.get(table_name)
    if cached is not None:
        return cached
    try:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                  AND column_name = 'instance_id'
            )
            """,
            (table_name,),
        )
        row = cursor.fetchone()
        has_col = bool(row and row[0])
    except Exception:
        has_col = False
    _table_has_instance_id_cache[table_name] = has_col
    return has_col


def _ensure_instance_id_column(table_name: str) -> bool:
    """
    Best-effort self-heal: ensure `instance_id` exists in target table.
    Returns True if column exists after this attempt.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS instance_id TEXT"
        )
        conn.commit()
        _table_has_instance_id_cache.pop(table_name, None)
        return _table_has_instance_id(cursor, table_name)
    except Exception:
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def init_db():
    """Crea tablas si no existen. En Supabase no se crea la base (ya existe)."""
    if not DATABASE_URL:
        print("❌ No se puede inicializar: falta DATABASE_URL o SUPABASE_DB_URL")
        return
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS drivers (
            steam_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS lap_records (
            id SERIAL PRIMARY KEY,
            steam_id VARCHAR(50),
            car_model VARCHAR(100),
            track VARCHAR(100),
            track_config VARCHAR(255) DEFAULT '',
            server_name VARCHAR(100),
            lap_time INTEGER NOT NULL,
            valid_lap SMALLINT DEFAULT 1,
            "timestamp" BIGINT DEFAULT 0,
            "date" TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT lap_records_unique_lap UNIQUE (steam_id, car_model, track, track_config)
        )
        """
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS touge_battles (
            id SERIAL PRIMARY KEY,
            server_name VARCHAR(100),
            track VARCHAR(100),
            track_config VARCHAR(255) DEFAULT '',
            player1_steam_id VARCHAR(50),
            player2_steam_id VARCHAR(50),
            player1_car VARCHAR(100) DEFAULT '',
            player2_car VARCHAR(100) DEFAULT '',
            winner_steam_id VARCHAR(50) DEFAULT NULL,
            player1_score INTEGER DEFAULT 0,
            player2_score INTEGER DEFAULT 0,
            points_log JSONB DEFAULT NULL,
            status VARCHAR(20) DEFAULT 'active',
            started_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT touge_battles_status_chk CHECK (status IN ('active', 'finished'))
        )
        """
        )

        # Alineado con Drizzle / panel ac-data (lectura de eventos y batallas)
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS server_events (
            id BIGSERIAL PRIMARY KEY,
            event_id TEXT UNIQUE,
            server_name TEXT NOT NULL,
            webhook_url TEXT,
            webhook_secret TEXT,
            event_type TEXT,
            event_status TEXT DEFAULT 'started',
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
        )
        cursor.execute(
            "ALTER TABLE IF EXISTS server_events ADD COLUMN IF NOT EXISTS instance_id TEXT"
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS server_battles (
            id BIGSERIAL PRIMARY KEY,
            battle_id TEXT NOT NULL UNIQUE,
            server_name TEXT NOT NULL,
            webhook_url TEXT,
            webhook_secret TEXT,
            player1_steam_id TEXT NOT NULL,
            player2_steam_id TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
        )
        cursor.execute(
            "ALTER TABLE IF EXISTS server_battles ADD COLUMN IF NOT EXISTS instance_id TEXT"
        )

        cursor.close()
        conn.close()
        print("✅ Esquema PostgreSQL comprobado/creado (Supabase).")
    except Exception as e:
        print(f"❌ Error inicializando la base de datos: {e}")


def save_driver(steam_id, name, car_model):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO drivers (steam_id, name)
        VALUES (%s, %s)
        ON CONFLICT (steam_id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = NOW()
        """
        cursor.execute(query, (steam_id, name))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error saving driver: {e}")


def save_lap(steam_id, car_model, track, track_config, server_name, lap_time, valid, timestamp=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if timestamp is None:
            timestamp = int(time.time() * 1000)

        valid_int = 1 if valid else 0
        new_id = _next_lap_record_id(cursor)
        use_ov = _lap_id_needs_overriding(cursor)
        between = "\n    OVERRIDING SYSTEM VALUE\n    " if use_ov else "\n    "
        # Columna `date` en Drizzle es text; ISO evita null si la columna pasó a NOT NULL en algún deploy.
        date_str = datetime.now(timezone.utc).isoformat()

        query = f"""
        INSERT INTO lap_records (
            id, steam_id, car_model, track, track_config, server_name, lap_time, valid_lap, "timestamp", "date"
        ){between}VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (steam_id, car_model, track, track_config) DO UPDATE SET
            lap_time = CASE
                WHEN EXCLUDED.lap_time < lap_records.lap_time THEN EXCLUDED.lap_time
                ELSE lap_records.lap_time
            END,
            valid_lap = CASE
                WHEN EXCLUDED.lap_time < lap_records.lap_time THEN EXCLUDED.valid_lap
                ELSE lap_records.valid_lap
            END,
            "timestamp" = CASE
                WHEN EXCLUDED.lap_time < lap_records.lap_time THEN EXCLUDED."timestamp"
                ELSE lap_records."timestamp"
            END,
            server_name = EXCLUDED.server_name,
            "date" = CASE
                WHEN EXCLUDED.lap_time < lap_records.lap_time THEN EXCLUDED."date"
                ELSE lap_records."date"
            END
        """
        cursor.execute(
            query,
            (
                new_id,
                steam_id,
                car_model,
                track,
                track_config,
                server_name,
                lap_time,
                valid_int,
                timestamp,
                date_str,
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Lap saved for {steam_id}: {lap_time}ms (Valid: {valid}) - Route: {track_config}")
    except Exception as e:
        print(f"❌ Error saving lap: {e}")


def start_touge_battle(server_name, track, track_config, p1_guid, p2_guid, p1_car="", p2_car=""):
    """Insert a new battle when it becomes ACTIVE. Returns the new battle_id."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO touge_battles (server_name, track, track_config, player1_steam_id, player2_steam_id, player1_car, player2_car, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')
        RETURNING id
        """
        cursor.execute(query, (server_name, track, track_config, p1_guid, p2_guid, p1_car, p2_car))
        row = cursor.fetchone()
        battle_id = row[0] if row else None
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Battle #{battle_id} started: {p1_guid} ({p1_car}) vs {p2_guid} ({p2_car}) on {track}")
        return battle_id
    except Exception as e:
        print(f"❌ Error starting Touge Battle: {e}")
        return None


def update_touge_score(battle_id, p1_score, p2_score, winner_guid=None, points_log=None):
    """Update the live score for a battle. Call this after every point."""
    if battle_id is None:
        return
    try:
        conn = get_connection()
        cursor = conn.cursor()
        status = "finished" if winner_guid else "active"
        log_json = Json(points_log) if points_log is not None else None
        query = """
        UPDATE touge_battles
        SET player1_score=%s, player2_score=%s, winner_steam_id=%s, status=%s, points_log=%s, updated_at=NOW()
        WHERE id=%s
        """
        cursor.execute(query, (p1_score, p2_score, winner_guid, status, log_json, battle_id))
        conn.commit()
        cursor.close()
        conn.close()
        flag = f"🏆 Winner: {winner_guid}" if winner_guid else f"Score: {p1_score}-{p2_score}"
        print(f"💾 Battle #{battle_id} updated — {flag}")
    except Exception as e:
        print(f"❌ Error updating Touge Battle score: {e}")


def save_touge_battle(server_name, track, track_config, p1_guid, p2_guid, winner_guid, p1_score, p2_score):
    """Legacy: save a complete battle at the end (used if no battle_id was set)."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO touge_battles (server_name, track, track_config, player1_steam_id, player2_steam_id, winner_steam_id, player1_score, player2_score, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'finished')
        """
        cursor.execute(
            query,
            (server_name, track, track_config, p1_guid, p2_guid, winner_guid, p1_score, p2_score),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Touge Battle saved: Winner {winner_guid} | [{p1_score}-{p2_score}] on {track}")
    except Exception as e:
        print(f"❌ Error saving Touge Battle: {e}")


_event_cache = {}
_server_active_cache = {}
_server_mode_cache = {}
_instance_gate_warned = False


def is_server_active_for_instance(server_name: str) -> bool:
    """
    Verifica en ac_server_control que este server esté activo para ESTA instancia.
    Si AC_INSTANCE_ID no está configurado o falta la tabla/columnas, hace fallback True
    para no romper setups antiguos.
    """
    name = (server_name or "").strip()
    if not name:
        return False
    global _instance_gate_warned
    if not AC_INSTANCE_ID:
        if not _instance_gate_warned:
            print("⚠️ AC_INSTANCE_ID no definido: se bloquean battle/event por seguridad de instancia.")
            _instance_gate_warned = True
        return False

    cache_key = f"{name}_{AC_INSTANCE_ID}"
    now = time.time()
    if cache_key in _server_active_cache:
        val, ts = _server_active_cache[cache_key]
        if now - ts < 3.0:
            return val

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'ac_server_control'
            )
            """
        )
        has_table = bool(cursor.fetchone()[0])
        if not has_table:
            _server_active_cache[cache_key] = (False, now)
            return False

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM ac_server_control
                WHERE instance_id = %s
                  AND server_name = %s
                  AND COALESCE(power_state, 'stopped') = 'running'
            )
            """,
            (AC_INSTANCE_ID, name),
        )
        is_active = bool(cursor.fetchone()[0])
        _server_active_cache[cache_key] = (is_active, now)
        return is_active
    except Exception as e:
        print(f"⚠️ Instance gate error ({name}/{AC_INSTANCE_ID}): {e}")
        _server_active_cache[cache_key] = (False, now)
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_server_mode_for_instance(server_name: str) -> Optional[str]:
    """
    Lee el modo operativo del server desde ac_server_control.
    Prioridad de columnas:
      1) server_type
      2) event_type
    Retorna: 'battle', 'time-attack', 'event'.
    Fallback por defecto: 'time-attack' cuando no hay modo explícito.
    """
    name = (server_name or "").strip()
    if not name or not AC_INSTANCE_ID:
        return "time-attack"

    cache_key = f"{name}_{AC_INSTANCE_ID}"
    now = time.time()
    if cache_key in _server_mode_cache:
        val, ts = _server_mode_cache[cache_key]
        if now - ts < 3.0:
            return val

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'ac_server_control'
            )
            """
        )
        if not bool(cursor.fetchone()[0]):
            _server_mode_cache[cache_key] = ("time-attack", now)
            return "time-attack"

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='ac_server_control'
            """
        )
        available_cols = {r[0] for r in cursor.fetchall() or []}
        if "server_type" in available_cols:
            mode_col = "server_type"
        elif "event_type" in available_cols:
            mode_col = "event_type"
        else:
            _server_mode_cache[cache_key] = ("time-attack", now)
            return "time-attack"

        order_terms = []
        if "updated_at" in available_cols:
            order_terms.append("updated_at DESC NULLS LAST")
        if "created_at" in available_cols:
            order_terms.append("created_at DESC NULLS LAST")
        if "id" in available_cols:
            order_terms.append("id DESC")
        if not order_terms:
            # No reliable ordering columns in schema; still return first matching row.
            order_terms.append(mode_col)
        order_clause = ", ".join(order_terms)

        cursor.execute(
            f"""
            SELECT {mode_col}
            FROM ac_server_control
            WHERE instance_id = %s
              AND lower(btrim(server_name)) = lower(btrim(%s))
              AND lower(COALESCE(power_state, 'stopped')) IN ('running', 'started', 'online')
            ORDER BY {order_clause}
            LIMIT 1
            """,
            (AC_INSTANCE_ID, name),
        )
        row = cursor.fetchone()
        if not row:
            _server_mode_cache[cache_key] = ("time-attack", now)
            return "time-attack"

        mode = (row[0] or "").strip().lower().replace("_", "-")
        if mode not in {"battle", "event", "time-attack"}:
            mode = "time-attack"
        _server_mode_cache[cache_key] = (mode, now)
        return mode
    except Exception as e:
        print(f"⚠️ Server mode lookup error ({name}/{AC_INSTANCE_ID}): {e}")
        _server_mode_cache[cache_key] = ("time-attack", now)
        return "time-attack"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_active_server_event(server_name, event_type=None):
    """
    Retorna la configuración del webhook activo para el server dado. Si event_type es
    None, retorna cualquier evento activo del servidor (el más reciente).
    Usa un cache corto de 3 segundos para no saturar la BD.
    """
    cache_key = f"{server_name}_{event_type}_{AC_INSTANCE_ID or '-'}"
    now = time.time()

    if cache_key in _event_cache:
        cached_result, cached_time = _event_cache[cache_key]
        if now - cached_time < 3.0:
            return cached_result

    if not is_server_active_for_instance(server_name):
        _event_cache[cache_key] = (None, now)
        return None

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        has_instance_id = _table_has_instance_id(cursor, "server_events")
        if AC_INSTANCE_ID and not has_instance_id:
            has_instance_id = _ensure_instance_id_column("server_events")
        if AC_INSTANCE_ID and not has_instance_id:
            # Fail-closed silently: ignore events if instance_id isolation is unavailable.
            _event_cache[cache_key] = (None, now)
            return None
        if AC_INSTANCE_ID and has_instance_id:
            instance_clause = " AND instance_id = %s"
            instance_params = (AC_INSTANCE_ID,)
        else:
            instance_clause = ""
            instance_params = ()
        if event_type:
            query = f"""
                SELECT webhook_url, event_type, metadata, event_status
                FROM server_events
                WHERE server_name = %s AND event_type = %s AND event_status = 'started'{instance_clause}
                ORDER BY id DESC LIMIT 1
            """
            cursor.execute(query, (server_name, event_type, *instance_params))
        else:
            query = f"""
                SELECT webhook_url, event_type, metadata, event_status
                FROM server_events
                WHERE server_name = %s AND event_status = 'started'{instance_clause}
                ORDER BY id DESC LIMIT 1
            """
            cursor.execute(query, (server_name, *instance_params))

        row = cursor.fetchone()
        if row:
            meta = row["metadata"] if row["metadata"] else {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except json.JSONDecodeError:
                    meta = {}
            res = {
                "webhook_url": row["webhook_url"],
                "event_type": row["event_type"],
                "metadata": meta,
            }
            _event_cache[cache_key] = (res, now)
            return res
        _event_cache[cache_key] = (None, now)
        return None
    except Exception as e:
        print(f"❌ Error getting active server event: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return None


_battle_cache = {}
_battle_config_deprecated_warned = False


def get_active_battle_config(server_name):
    """
    Deprecated: battle mode no longer depends on `server_battles`.
    Kept only for backward compatibility with old callers.
    """
    global _battle_config_deprecated_warned
    if not _battle_config_deprecated_warned:
        print("ℹ️ get_active_battle_config() deprecated: using server mode from ac_server_control.")
        _battle_config_deprecated_warned = True
    return None


def list_all_server_events():
    """Retorna todos los registros de server_events para poder elegir el servidor correcto."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT id, server_name, event_type, webhook_url, created_at
            FROM server_events
            ORDER BY id DESC
            """
        )
        return cursor.fetchall()
    except Exception as e:
        print(f"❌ Error listing server events: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return []

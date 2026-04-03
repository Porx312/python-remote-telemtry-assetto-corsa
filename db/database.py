import json
import os
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import psycopg2
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import Json, RealDictCursor

load_dotenv()

# Supabase Postgres: Project Settings → Database → URI (usa sslmode=require)
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL", "")

if os.getenv("USE_TEST_DB", "false").lower() == "true":
    DATABASE_URL = os.getenv("TEST_DATABASE_URL") or DATABASE_URL


def _ensure_sslmode(url: str) -> str:
    """Supabase exige SSL; añade sslmode=require si no viene en la URL."""
    if not url:
        return url
    parsed = urlparse(url)
    if parsed.scheme not in ("postgresql", "postgres"):
        return url
    q = parse_qs(parsed.query, keep_blank_values=True)
    low = {k.lower(): v for k, v in q.items()}
    if "sslmode" not in low and "gssencmode" not in low:
        q["sslmode"] = ["require"]
    # parse_qs devuelve listas; urlencode las aplana
    flat = []
    for k, vals in q.items():
        for v in vals:
            flat.append((k, v))
    new_query = urlencode(flat)
    return urlunparse(parsed._replace(query=new_query))


DATABASE_URL = _ensure_sslmode(DATABASE_URL)

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
except psycopg2.Error as err:
    print(f"❌ Error al crear el pool de conexiones: {err}")


class _PooledConn:
    """Devuelve la conexión al pool al llamar a close() (comportamiento tipo mysql-connector)."""

    __slots__ = ("_raw",)

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def close(self):
        if db_pool and self._raw:
            db_pool.putconn(self._raw)
            self._raw = None


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL / SUPABASE_DB_URL no configurado")
    if db_pool:
        return _PooledConn(db_pool.getconn())
    return psycopg2.connect(DATABASE_URL)


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
        query = """
        INSERT INTO lap_records (steam_id, car_model, track, track_config, server_name, lap_time, valid_lap, "timestamp")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
            server_name = EXCLUDED.server_name
        """
        cursor.execute(
            query,
            (steam_id, car_model, track, track_config, server_name, lap_time, valid_int, timestamp),
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


def get_active_server_event(server_name, event_type=None):
    """
    Retorna la configuración del webhook activo para el server dado. Si event_type es
    None, retorna cualquier evento activo del servidor (el más reciente).
    Usa un cache corto de 3 segundos para no saturar la BD.
    """
    cache_key = f"{server_name}_{event_type}"
    now = time.time()

    if cache_key in _event_cache:
        cached_result, cached_time = _event_cache[cache_key]
        if now - cached_time < 3.0:
            return cached_result

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        if event_type:
            query = """
                SELECT webhook_url, event_type, metadata, event_status
                FROM server_events
                WHERE server_name = %s AND event_type = %s AND event_status = 'started'
                ORDER BY id DESC LIMIT 1
            """
            cursor.execute(query, (server_name, event_type))
        else:
            query = """
                SELECT webhook_url, event_type, metadata, event_status
                FROM server_events
                WHERE server_name = %s AND event_status = 'started'
                ORDER BY id DESC LIMIT 1
            """
            cursor.execute(query, (server_name,))

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


def get_active_battle_config(server_name):
    """
    Retorna la configuración de la batalla activa (desde server_battles) para el server dado.
    Usa cache corto de 3 segundos.
    """
    now = time.time()
    if server_name in _battle_cache:
        cached_result, cached_time = _battle_cache[server_name]
        if now - cached_time < 3.0:
            return cached_result

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT battle_id, player1_steam_id, player2_steam_id, webhook_url, webhook_secret, metadata
            FROM server_battles
            WHERE server_name = %s AND status = 'active'
            ORDER BY id DESC LIMIT 1
        """
        cursor.execute(query, (server_name,))
        row = cursor.fetchone()

        if row:
            meta = row["metadata"] if row["metadata"] else {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except json.JSONDecodeError:
                    meta = {}
            res = {
                "battle_id": row["battle_id"],
                "player1_steam_id": row["player1_steam_id"],
                "player2_steam_id": row["player2_steam_id"],
                "webhook_url": row["webhook_url"],
                "webhook_secret": row["webhook_secret"],
                "metadata": meta,
            }
            _battle_cache[server_name] = (res, now)
            return res
        _battle_cache[server_name] = (None, now)
        return None
    except Exception as e:
        print(f"❌ Error getting active battle config: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
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

#!/usr/bin/env python3
"""
Migra `drivers` y `lap_records` desde MySQL (local u otro host) hacia Supabase (PostgreSQL).

Origen (MySQL) — variables de entorno (compatibles con el .env antiguo):
  DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT
  o: MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT

Destino (Supabase):
  DATABASE_URL o SUPABASE_DB_URL (postgresql://...)

Uso:
  python scripts/migrate_mysql_to_supabase.py
  python scripts/migrate_mysql_to_supabase.py --dry-run
  python scripts/migrate_mysql_to_supabase.py --only drivers
  python scripts/migrate_mysql_to_supabase.py --only laps --batch-size 200

Si `lap_records` viene de Drizzle sin UNIQUE en (steam_id, car_model, track, track_config),
el script intenta crear `lap_records_unique_lap` antes de los INSERT … ON CONFLICT.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import mysql.connector
import psycopg2
from dotenv import load_dotenv
from mysql.connector.cursor import MySQLCursorDict
from psycopg2.extras import execute_batch

load_dotenv()


def ensure_sslmode(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if parsed.scheme not in ("postgresql", "postgres"):
        return url
    q = parse_qs(parsed.query, keep_blank_values=True)
    low = {k.lower(): v for k, v in q.items()}
    if "sslmode" not in low and "gssencmode" not in low:
        q["sslmode"] = ["require"]
    flat = []
    for k, vals in q.items():
        for v in vals:
            flat.append((k, v))
    return urlunparse(parsed._replace(query=urlencode(flat)))


def mysql_config():
    return {
        "host": os.getenv("MYSQL_HOST") or os.getenv("DB_HOST", "localhost"),
        "user": os.getenv("MYSQL_USER") or os.getenv("DB_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD") or os.getenv("DB_PASS", ""),
        "database": os.getenv("MYSQL_DATABASE") or os.getenv("DB_NAME", "ac_server_db"),
        "port": int(os.getenv("MYSQL_PORT") or os.getenv("DB_PORT", "3306")),
    }


def pg_dsn() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL", "")
    return ensure_sslmode(url)


def _norm_ts(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return value


def _norm_track_config(v):
    if v is None:
        return ""
    return str(v)


def _lap_records_id_is_identity(pg_cur) -> bool:
    """Si `id` es IDENTITY, Postgres ignora valores explícitos salvo OVERRIDING SYSTEM VALUE."""
    try:
        pg_cur.execute(
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
        row = pg_cur.fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def build_lap_upsert_sql(use_overriding_identity: bool) -> str:
    """INSERT … ON CONFLICT para lap_records."""
    between_cols_and_values = (
        "\n    OVERRIDING SYSTEM VALUE\n    "
        if use_overriding_identity
        else "\n    "
    )
    return f"""
    INSERT INTO lap_records (
        id, steam_id, car_model, track, track_config, server_name,
        lap_time, valid_lap, "timestamp", "date"
    ){between_cols_and_values}VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (steam_id, car_model, track, track_config) DO UPDATE SET
        id = CASE
            WHEN EXCLUDED.lap_time < lap_records.lap_time THEN EXCLUDED.id
            ELSE lap_records.id
        END,
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
        server_name = CASE
            WHEN EXCLUDED.lap_time < lap_records.lap_time THEN EXCLUDED.server_name
            ELSE lap_records.server_name
        END,
        "date" = CASE
            WHEN EXCLUDED.lap_time < lap_records.lap_time THEN EXCLUDED."date"
            ELSE lap_records."date"
        END
    """


def ensure_lap_records_unique_for_upsert(pg_cur) -> None:
    """
    ON CONFLICT requiere un índice/constraint UNIQUE en esas columnas.
    Drizzle (ac-data) crea `lap_records` sin ese UNIQUE; lo añadimos si falta.
    """
    pg_cur.execute(
        """
        UPDATE lap_records SET track_config = COALESCE(track_config, '')
        WHERE track_config IS NULL
        """
    )
    pg_cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS lap_records_unique_lap
        ON lap_records (steam_id, car_model, track, track_config)
        """
    )


def migrate_drivers(mysql_cur, pg_cur, dry_run: bool, page_size: int = 500) -> int:
    mysql_cur.execute(
        "SELECT steam_id, name, created_at, updated_at FROM drivers ORDER BY steam_id"
    )
    rows = mysql_cur.fetchall()
    if dry_run:
        return len(rows)

    upsert_sql = """
    INSERT INTO drivers (steam_id, name, created_at, updated_at)
    VALUES (%s, %s, COALESCE(%s, NOW()), COALESCE(%s, NOW()))
    ON CONFLICT (steam_id) DO UPDATE SET
        name = EXCLUDED.name,
        updated_at = COALESCE(EXCLUDED.updated_at, drivers.updated_at),
        created_at = LEAST(COALESCE(drivers.created_at, NOW()), COALESCE(EXCLUDED.created_at, NOW()))
    """
    batch = []
    for r in rows:
        batch.append(
            (
                r["steam_id"],
                r["name"],
                _norm_ts(r.get("created_at")),
                _norm_ts(r.get("updated_at")),
            )
        )
    if batch:
        execute_batch(pg_cur, upsert_sql, batch, page_size=page_size)
    return len(batch)


def _mysql_row_int_id(row: dict, key: str) -> int | None:
    v = row.get(key)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _ensure_lap_row_ids(rows: list) -> None:
    """
    Postgres exige `id` NOT NULL. Rellena ids faltantes (MySQL raro o clave distinta en el dict).
    Modifica cada dict in-place añadiendo `_pg_id` int.
    """
    mx = 0
    for r in rows:
        rid = _mysql_row_int_id(r, "lap_row_id") or _mysql_row_int_id(r, "id")
        if rid is not None:
            mx = max(mx, rid)
    synth = mx
    missing = 0
    for r in rows:
        rid = _mysql_row_int_id(r, "lap_row_id") or _mysql_row_int_id(r, "id")
        if rid is None:
            synth += 1
            rid = synth
            missing += 1
        r["_pg_id"] = rid
    if missing and os.getenv("MIGRATE_DEBUG"):
        print(f"⚠️  {missing} filas sin id en MySQL; ids sintéticos a partir de {mx + 1}", file=sys.stderr)
    if rows and os.getenv("MIGRATE_DEBUG"):
        print("DEBUG claves primera fila MySQL:", list(rows[0].keys()), file=sys.stderr)


def migrate_laps(mysql_cur, pg_cur, dry_run: bool, page_size: int = 500) -> int:
    # lap_row_id: alias explícito (evita rarezas con la columna `id` en el cursor)
    mysql_cur.execute(
        """
        SELECT `id` AS lap_row_id, steam_id, car_model, track, COALESCE(track_config, '') AS track_config,
               server_name, lap_time, valid_lap, `timestamp`, `date`
        FROM lap_records
        ORDER BY `id`
        """
    )
    rows = mysql_cur.fetchall()
    if not dry_run:
        _ensure_lap_row_ids(rows)
    if dry_run:
        return len(rows)

    try:
        ensure_lap_records_unique_for_upsert(pg_cur)
    except Exception as e:
        print(
            "\n❌ No se pudo crear el índice único en lap_records (necesario para ON CONFLICT).\n"
            "   Suele ser por filas duplicadas con el mismo (steam_id, car_model, track, track_config).\n"
            "   Revisa en Supabase SQL, borra o fusiona duplicados, y vuelve a ejecutar.\n"
            f"   Detalle: {e}\n",
            file=sys.stderr,
        )
        raise

    # IDENTITY en Postgres: sin OVERRIDING SYSTEM VALUE el valor explícito de `id` se ignora → NULL → error NOT NULL.
    id_is_identity = _lap_records_id_is_identity(pg_cur)
    print(
        f"   lap_records.id es IDENTITY: {id_is_identity} "
        f"({'INSERT con OVERRIDING SYSTEM VALUE' if id_is_identity else 'INSERT explícito normal'})",
        flush=True,
    )

    upsert_sql = build_lap_upsert_sql(id_is_identity)
    batch = []
    for r in rows:
        vd = r.get("valid_lap")
        if vd is None:
            vd = 1
        else:
            vd = int(vd)
        ts = r.get("timestamp")
        if ts is not None:
            ts = int(ts)
        raw_date = r.get("date")
        if raw_date is None:
            lap_date_str = None
        elif isinstance(raw_date, datetime):
            lap_date_str = raw_date.isoformat(sep=" ", timespec="seconds")
        else:
            lap_date_str = str(raw_date)

        pid = int(r["_pg_id"])
        batch.append(
            (
                pid,
                r["steam_id"],
                r["car_model"],
                r["track"],
                _norm_track_config(r.get("track_config")),
                r.get("server_name"),
                int(r["lap_time"]),
                vd,
                ts,
                lap_date_str,
            )
        )
    if batch:
        # executemany/execute_batch + ON CONFLICT a veces fallan con PgBouncer/psycopg2; fila a fila es fiable.
        first = batch[0]
        if first[0] is None:
            raise RuntimeError("Bug interno: id nulo en la primera fila del batch")
        for i, tup in enumerate(batch):
            pg_cur.execute(upsert_sql, tup)
            if i == 0 and os.getenv("MIGRATE_DEBUG"):
                print(f"DEBUG primera fila enviada a Postgres: id={tup[0]!r} …", file=sys.stderr)
    return len(batch)


def main() -> int:
    parser = argparse.ArgumentParser(description="MySQL → Supabase: drivers y lap_records")
    parser.add_argument("--dry-run", action="store_true", help="Solo cuenta filas, no escribe en Supabase")
    parser.add_argument("--only", choices=("drivers", "laps", "all"), default="all")
    parser.add_argument("--batch-size", type=int, default=500, help="Filas por lote (execute_batch page_size)")
    args = parser.parse_args()

    dsn = pg_dsn()
    if not dsn and not args.dry_run:
        print("❌ Falta DATABASE_URL o SUPABASE_DB_URL.", file=sys.stderr)
        return 1
    if args.dry_run and not dsn:
        print("⚠️  Dry-run: se omitirá conexión a Supabase; solo se leerá MySQL.")

    mcfg = mysql_config()
    print(f"📥 MySQL: {mcfg['user']}@{mcfg['host']}:{mcfg['port']}/{mcfg['database']}")
    if dsn and not args.dry_run:
        p = urlparse(dsn)
        print(f"📤 Supabase: {p.scheme}://{p.hostname}:{p.port or 5432}{p.path}")

    try:
        mconn = mysql.connector.connect(**mcfg)
    except mysql.connector.Error as e:
        print(f"❌ No se pudo conectar a MySQL: {e}", file=sys.stderr)
        return 1

    bs = max(1, args.batch_size)

    if args.dry_run:
        try:
            if args.only in ("drivers", "all"):
                cur = mconn.cursor(cursor_class=MySQLCursorDict)
                n = migrate_drivers(cur, None, True, page_size=bs)
                cur.close()
                print(f"   drivers: {n} filas (dry-run)")
            if args.only in ("laps", "all"):
                cur = mconn.cursor(cursor_class=MySQLCursorDict)
                n = migrate_laps(cur, None, True, page_size=bs)
                cur.close()
                print(f"   lap_records: {n} filas (dry-run)")
        finally:
            mconn.close()
        print("✅ Dry-run terminado.")
        return 0

    pconn = None
    pg_cur = None
    try:
        pconn = psycopg2.connect(dsn)
        pconn.autocommit = False
        pg_cur = pconn.cursor()

        if args.only in ("drivers", "all"):
            mysql_cur = mconn.cursor(cursor_class=MySQLCursorDict)
            n_drivers = migrate_drivers(mysql_cur, pg_cur, False, page_size=bs)
            mysql_cur.close()
            print(f"✅ drivers migrados: {n_drivers}")

        if args.only in ("laps", "all"):
            mysql_cur = mconn.cursor(cursor_class=MySQLCursorDict)
            n_laps = migrate_laps(mysql_cur, pg_cur, False, page_size=bs)
            mysql_cur.close()
            print(f"✅ lap_records migrados: {n_laps}")

        pconn.commit()
        print("✅ Migración completada.")
        return 0
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if pconn is not None:
            try:
                pconn.rollback()
            except Exception:
                pass
        return 1
    finally:
        if pg_cur is not None:
            try:
                pg_cur.close()
            except Exception:
                pass
        if pconn is not None:
            try:
                pconn.close()
            except Exception:
                pass
        try:
            mconn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

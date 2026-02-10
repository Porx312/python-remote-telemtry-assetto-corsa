import mysql.connector
import os
import time
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root') # Default fallback
DB_NAME = os.getenv('DB_NAME', 'ac_telemetry')
DB_PORT = int(os.getenv('DB_PORT', 3306))

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=DB_PORT
    )

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    # Ensure tables exist (Basic check, ideal schema provided in previous tasks)
    # Here we assume the DB is already set up by the Node.js project or external tools.
    conn.close()
    print("✅ Database connection verified.")

def save_driver(steam_id, name, car_model):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO drivers (steam_id, name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            updated_at = NOW()
        """
        cursor.execute(query, (steam_id, name))
        conn.commit()
        cursor.close()
        conn.close()
        # print(f"💾 Driver saved: {name} ({steam_id})")
    except Exception as e:
        print(f"❌ Error saving driver: {e}")

def save_lap(steam_id, car_model, track, server_name, lap_time, valid, timestamp=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        if timestamp is None:
            timestamp = int(time.time() * 1000)

        # ON DUPLICATE KEY UPDATE: keep the best (lowest) lap time
        query = """
        INSERT INTO lap_records (steam_id, car_model, track, server_name, lap_time, valid_lap, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            lap_time = IF(%s < lap_time, %s, lap_time),
            valid_lap = IF(%s < lap_time, %s, valid_lap),
            timestamp = IF(%s < lap_time, %s, timestamp),
            server_name = VALUES(server_name)
        """
        valid_int = 1 if valid else 0
        cursor.execute(query, (
            steam_id, car_model, track, server_name, lap_time, valid_int, timestamp,
            lap_time, lap_time,      # lap_time update
            lap_time, valid_int,     # valid_lap update
            lap_time, timestamp      # timestamp update
        ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Lap saved for {steam_id}: {lap_time}ms (Valid: {valid})")
    except Exception as e:
        print(f"❌ Error saving lap: {e}")

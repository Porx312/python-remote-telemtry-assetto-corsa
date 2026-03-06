import mysql.connector
import os
import time
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root') # Default fallback
DB_NAME = os.getenv('DB_NAME', 'ac_server_db')
DB_NAME_TEST = os.getenv('DB_NAME_TEST', 'ac_server_db')
DB_PORT = int(os.getenv('DB_PORT', 3306))

USE_TEST_DB = os.getenv('USE_TEST_DB', 'true').lower() == 'true'

def get_db_name():
    return DB_NAME_TEST if USE_TEST_DB else DB_NAME

def get_connection(db_name=None):
    if db_name is None:
        db_name = get_db_name()
        
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=db_name,
        port=DB_PORT
    )

def init_db():
    try:
        # First connect without DB to create it if it doesn't exist
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cursor = conn.cursor()
        db_name = get_db_name()
        
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        cursor.execute(f"USE `{db_name}`")
        
        # Create drivers table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            steam_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """)
        
        # Create lap_records table with track_config
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS lap_records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            steam_id VARCHAR(50),
            car_model VARCHAR(100),
            track VARCHAR(100),
            track_config VARCHAR(255) DEFAULT '',
            server_name VARCHAR(100),
            lap_time INT NOT NULL,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_lap TINYINT(1) DEFAULT 1,
            timestamp BIGINT DEFAULT 0,
            UNIQUE KEY unique_lap (steam_id, car_model, track, track_config)
        )
        """)

        # Create touge_battles table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS touge_battles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            server_name VARCHAR(100),
            track VARCHAR(100),
            track_config VARCHAR(255) DEFAULT '',
            player1_steam_id VARCHAR(50),
            player2_steam_id VARCHAR(50),
            player1_car VARCHAR(100) DEFAULT '',
            player2_car VARCHAR(100) DEFAULT '',
            winner_steam_id VARCHAR(50) DEFAULT NULL,
            player1_score INT DEFAULT 0,
            player2_score INT DEFAULT 0,
            points_log JSON DEFAULT NULL,
            status ENUM('active','finished') DEFAULT 'active',
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """)
        
        # Safely add the columns if the table was created before this update
        try:
            cursor.execute("ALTER TABLE touge_battles ADD COLUMN player1_car VARCHAR(100) DEFAULT '' AFTER player2_steam_id")
            cursor.execute("ALTER TABLE touge_battles ADD COLUMN player2_car VARCHAR(100) DEFAULT '' AFTER player1_car")
        except:
            pass # Columns already exist
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Database connection verified. Using database: {db_name}")
    except Exception as e:
        print(f"❌ Error initializing database: {e}")

def save_driver(steam_id, name, car_model):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO drivers (steam_id, name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name)
        """
        cursor.execute(query, (steam_id, name))
        conn.commit()
        cursor.close()
        conn.close()
        # print(f"💾 Driver saved: {name} ({steam_id})")
    except Exception as e:
        print(f"❌ Error saving driver: {e}")

def save_lap(steam_id, car_model, track, track_config, server_name, lap_time, valid, timestamp=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        if timestamp is None:
            timestamp = int(time.time() * 1000)

        # ON DUPLICATE KEY UPDATE: keep the best (lowest) lap time
        query = """
        INSERT INTO lap_records (steam_id, car_model, track, track_config, server_name, lap_time, valid_lap, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            lap_time = IF(%s < lap_time, %s, lap_time),
            valid_lap = IF(%s < lap_time, %s, valid_lap),
            timestamp = IF(%s < lap_time, %s, timestamp),
            server_name = VALUES(server_name)
        """
        valid_int = 1 if valid else 0
        cursor.execute(query, (
            steam_id, car_model, track, track_config, server_name, lap_time, valid_int, timestamp,
            lap_time, lap_time,      # lap_time update
            lap_time, valid_int,     # valid_lap update
            lap_time, timestamp      # timestamp update
        ))
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
        """
        cursor.execute(query, (server_name, track, track_config, p1_guid, p2_guid, p1_car, p2_car))
        conn.commit()
        battle_id = cursor.lastrowid
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
        import json
        conn = get_connection()
        cursor = conn.cursor()
        status = 'finished' if winner_guid else 'active'
        log_json = json.dumps(points_log) if points_log is not None else None
        query = """
        UPDATE touge_battles
        SET player1_score=%s, player2_score=%s, winner_steam_id=%s, status=%s, points_log=%s
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
        cursor.execute(query, (server_name, track, track_config, p1_guid, p2_guid, winner_guid, p1_score, p2_score))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Touge Battle saved: Winner {winner_guid} | [{p1_score}-{p2_score}] on {track}")
    except Exception as e:
        print(f"❌ Error saving Touge Battle: {e}")

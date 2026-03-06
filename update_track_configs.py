import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = 'ac_server_db'  # Change this if your old DB has another name
DB_PORT = int(os.getenv('DB_PORT', 3306))

# --- Define the missing track configs here ---
# Example: "track_name_in_db": "config_name"
TRACK_CONFIG_MAPPING = {
    "pk_akina": "akina_downhill",
    "pk_usui_pass": "usuipass_short_dh",
    "ek_akagi": "downhill_real",
    "ek_myogi":"downhill_real"
    # Add your tracks and configs here!
}

def main():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            port=DB_PORT
        )
        cursor = conn.cursor()
        
        # 1. Ensure column exists first (prevent errors if applying to old DB schema for the first time)
        try:
            cursor.execute("SELECT track_config FROM lap_records LIMIT 1")
            cursor.fetchall()
        except mysql.connector.Error as e:
            if e.errno == 1054: # Unknown column
                print("⚠️ Column 'track_config' missing. Adding to lap_records...")
                cursor.execute("ALTER TABLE lap_records ADD COLUMN track_config VARCHAR(255) DEFAULT ''")
                conn.commit()
            else:
                raise e

        # 2. Get distinct tracks that need updating
        cursor.execute("SELECT DISTINCT track FROM lap_records WHERE track_config IS NULL OR track_config = ''")
        tracks_needing_update = [r[0] for r in cursor.fetchall()]

        if not tracks_needing_update:
            print("✅ No records found missing a track configuration.")
            return

        print(f"🔍 Found {len(tracks_needing_update)} tracks with empty configurations:")
        for t in tracks_needing_update:
            print(f"  - {t}")

        # 3. Apply configurations based on mapping
        updated_count = 0
        for track in tracks_needing_update:
            if track in TRACK_CONFIG_MAPPING:
                config = TRACK_CONFIG_MAPPING[track]
                cursor.execute(
                    "UPDATE lap_records SET track_config = %s WHERE track = %s AND (track_config IS NULL OR track_config = '')", 
                    (config, track)
                )
                affected = cursor.rowcount
                print(f"✅ Updated {affected} records for track '{track}' -> config: '{config}'")
                updated_count += affected
            else:
                print(f"⚠️ Skipping '{track}' - No configuration defined in TRACK_CONFIG_MAPPING.")

        if updated_count > 0:
            conn.commit()
            print(f"🎉 Successfully applied {updated_count} updates!")
        else:
            print("ℹ️ No changes made.")

    except Exception as e:
        print(f"❌ Database error: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    main()

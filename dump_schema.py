import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()
conn = mysql.connector.connect(host=os.getenv('DB_HOST','localhost'), user=os.getenv('DB_USER','root'), password=os.getenv('DB_PASS','root'), database='ac_server_db', port=int(os.getenv('DB_PORT', 3306)))
cursor = conn.cursor()
cursor.execute('SHOW CREATE TABLE lap_records')
res = cursor.fetchone()[1]
with open('schema_debug.txt', 'w', encoding='utf-8') as f:
    f.write(res)

import pymysql
import sqlite3
import sys
import time
import os

# Windows Unicode Fix
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

TIDB_HOST = 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com'
TIDB_PORT = 4000
TIDB_USER = 'ktQaY5Y9x7TFRvV.root'
TIDB_PASS = 'qUlM80c5MitEvh2e'
TIDB_DB = 'test'

SQLITE_DB = 'master.db'

def init_local_master():
    print(f"🚀 Initializing Local SQLite Master: {SQLITE_DB}")
    
    # Connect to local SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_cursor = sqlite_conn.cursor()
    
    # Create the table schema (matching TiDB, but optimized for SQLite)
    sqlite_cursor.execute('''
        CREATE TABLE IF NOT EXISTS asddo_voters (
            id INTEGER PRIMARY KEY,
            epic_no TEXT UNIQUE,
            district TEXT,
            ac_no TEXT,
            part_no TEXT,
            serial_no INTEGER,
            voter_name TEXT,
            relative_details TEXT,
            uncollectable_reason TEXT,
            data TEXT
        )
    ''')
    sqlite_conn.commit()

    print("🔌 Connecting to TiDB to stream data...")
    tidb_conn = pymysql.connect(
        host=TIDB_HOST, port=TIDB_PORT, user=TIDB_USER, password=TIDB_PASS, database=TIDB_DB,
        ssl={'rejectUnauthorized': True},
        cursorclass=pymysql.cursors.SSCursor # Unbuffered streaming cursor (zero memory footprint)
    )

    start_time = time.time()
    total_inserted = 0
    batch_size = 10000
    batch = []

    try:
        with tidb_conn.cursor() as cursor:
            # We select all columns. The 'data' column is JSON, which comes across as a string.
            print("⏳ Running SELECT query on TiDB (this may take a moment to begin streaming)...")
            cursor.execute("SELECT id, epic_no, district, ac_no, part_no, serial_no, voter_name, relative_details, uncollectable_reason, data FROM asddo_voters")
            
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                
                batch.append(row)
                
                if len(batch) >= batch_size:
                    sqlite_cursor.executemany(
                        "INSERT OR IGNORE INTO asddo_voters VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        batch
                    )
                    sqlite_conn.commit()
                    total_inserted += len(batch)
                    batch = []
                    
                    if total_inserted % 500000 == 0:
                        elapsed = time.time() - start_time
                        print(f"✅ Downloaded and saved {total_inserted:,} rows to SQLite... ({elapsed:.1f}s)")
            
            # Insert any remaining rows
            if batch:
                sqlite_cursor.executemany(
                    "INSERT OR IGNORE INTO asddo_voters VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    batch
                )
                sqlite_conn.commit()
                total_inserted += len(batch)

        print(f"\n🎉 Download Complete! Total rows saved to local SQLite: {total_inserted:,}")
        
        # Create fast search indexes on the local SQLite DB
        print("⚡ Building local search indexes...")
        sqlite_cursor.execute("CREATE INDEX IF NOT EXISTS idx_ac_part ON asddo_voters(district, ac_no, part_no, serial_no)")
        sqlite_conn.commit()
        
        file_size_mb = os.path.getsize(SQLITE_DB) / (1024 * 1024)
        print(f"💾 Final SQLite Database Size: {file_size_mb:.2f} MB")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        tidb_conn.close()
        sqlite_conn.close()

if __name__ == '__main__':
    init_local_master()

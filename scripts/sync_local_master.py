import sqlite3
import json
import os
import sys
import time
import re
import zipfile
import io

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

SQLITE_DB = 'master.db'
EXTRACTED_DIR = r'D:\SRI WebApp\cache\extracted'
MANIFEST_FILE = r'D:\SRI WebApp\cache\manifest.json'

def sync_local_data():
    print(f"🚀 Starting Local Master Sync for August 14th Hybrid Data")
    
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    
    # --- 1. Identify Skipped Booths from Manifest ---
    skipped_booths = []
    print("🔍 Analyzing manifest.json to identify untouched booths (NYU)...")
    with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
        for district in manifest.get('districts', []):
            for ac in district.get('acs', []):
                for file in ac.get('files', []):
                    match = re.search(r'_(\d{2})_(\d{2})_(\d{4})', file.get('name', ''))
                    is_skipped = False
                    if match:
                        day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                        if year == 2026 and month == 8 and day < 12:
                            is_skipped = True
                    
                    if is_skipped:
                        skipped_booths.append((str(file.get('acNo')), str(file.get('partNo'))))

    print(f"✅ Found {len(skipped_booths):,} untouched booths (NYU).")

    cursor.execute("DROP TABLE IF EXISTS temp_skipped_booths")
    cursor.execute("CREATE TABLE temp_skipped_booths (ac_no TEXT, part_no TEXT)")
    cursor.executemany("INSERT INTO temp_skipped_booths VALUES (?, ?)", skipped_booths)
    cursor.execute("CREATE INDEX idx_skipped ON temp_skipped_booths(ac_no, part_no)")
    conn.commit()

    # --- 2. Load Extracted Data ---
    print("⏳ Creating temporary table for new August 14th EPICs...")
    cursor.execute("DROP TABLE IF EXISTS temp_aug14_epics")
    cursor.execute('''
        CREATE TABLE temp_aug14_epics (
            epic_no TEXT PRIMARY KEY,
            district TEXT,
            acName TEXT,
            partNo TEXT,
            slNoInPart INTEGER,
            voterName TEXT,
            relativeName TEXT,
            reason TEXT
        )
    ''')
    
    print("📂 Scanning extracted NDJSON files...")
    if not os.path.exists(EXTRACTED_DIR):
        print(f"❌ Extracted directory not found: {EXTRACTED_DIR}")
        return

    total_new_records = 0
    batch = []
    
    for root, _, files in os.walk(EXTRACTED_DIR):
        for file in files:
            if file.endswith('.ndjson'):
                filepath = os.path.join(root, file)
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            record = json.loads(line)
                            epic = record.get('epic')
                            if epic:
                                batch.append((
                                    epic,
                                    record.get('district'),
                                    record.get('acName'),
                                    str(record.get('partNo')),
                                    record.get('slNoInPart'),
                                    record.get('voterName'),
                                    record.get('relativeName'),
                                    record.get('reason')
                                ))
                                total_new_records += 1
                                
                                if len(batch) >= 50000:
                                    print(f"    ... inserting batch (Total new records scanned: {total_new_records:,})")
                                    cursor.executemany("INSERT OR IGNORE INTO temp_aug14_epics VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                                    conn.commit()
                                    batch = []
                        except Exception as e:
                            pass
                            
    if batch:
        cursor.executemany("INSERT OR IGNORE INTO temp_aug14_epics VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
        conn.commit()

    print(f"✅ Loaded {total_new_records:,} EPICs from August 14th data into temporary memory.")
    
    # --- OPTIMIZATION: Match NYU string names in Python to avoid SQLite full table scans ---
    print("🔍 Pre-calculating exact booth string names for NYU to optimize SQLite...")
    cursor.execute('SELECT DISTINCT ac_no, part_no FROM asddo_voters')
    unique_booths = cursor.fetchall()
    
    skipped_set = set((int(ac), int(part)) for ac, part in skipped_booths)
    nyu_string_booths = []
    
    for ac_str, part_str in unique_booths:
        try:
            ac_match = re.match(r'^\d+', str(ac_str))
            part_match = re.match(r'^\d+', str(part_str))
            if ac_match and part_match:
                if (int(ac_match.group(0)), int(part_match.group(0))) in skipped_set:
                    nyu_string_booths.append((ac_str, part_str))
        except Exception:
            pass

    cursor.execute('CREATE TEMP TABLE temp_nyu_strings (ac_no TEXT, part_no TEXT)')
    cursor.executemany('INSERT INTO temp_nyu_strings VALUES (?, ?)', nyu_string_booths)
    cursor.execute('CREATE INDEX idx_temp_nyu_str ON temp_nyu_strings(ac_no, part_no)')
    
    print("⚡ Cross-checking existing 11.58M rows in master.db (Using Native Indexes)...")
    start_time = time.time()
    
    cursor.execute('''
        UPDATE asddo_voters
        SET data = json_set(
            coalesce(data, '{}'), 
            '$.ASDDO_14_Aug', 
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM temp_nyu_strings 
                    WHERE temp_nyu_strings.ac_no = asddo_voters.ac_no 
                      AND temp_nyu_strings.part_no = asddo_voters.part_no
                ) THEN 'NYU'
                WHEN EXISTS (
                    SELECT 1 FROM temp_aug14_epics 
                    WHERE temp_aug14_epics.epic_no = asddo_voters.epic_no
                ) THEN 'Yes' 
                ELSE 'No' 
            END,
            '$.ASDDO_11_Aug', coalesce(json_extract(data, '$.ASDDO_11_Aug'), 'Yes'),
            '$.ASDDO_03to10_Aug', coalesce(json_extract(data, '$.ASDDO_03to10_Aug'), 'Yes')
        )
    ''')
    conn.commit()
    
    # --- 4. Insert Brand New Additions ---
    print("➕ Inserting brand new voters added in August 14th release...")
    cursor.execute('''
        INSERT INTO asddo_voters (epic_no, district, ac_no, part_no, serial_no, voter_name, relative_details, uncollectable_reason, data)
        SELECT 
            epic_no, district, acName, partNo, slNoInPart, voterName, relativeName, reason,
            json_object('ASDDO_03to10_Aug', 'No', 'ASDDO_11_Aug', 'No', 'ASDDO_14_Aug', 'Yes')
        FROM temp_aug14_epics
        WHERE epic_no NOT IN (SELECT epic_no FROM asddo_voters)
    ''')
    conn.commit()
    new_inserts = cursor.rowcount
    
    elapsed = time.time() - start_time
    print(f"🎉 Cross-check complete! Processed millions of rows locally in {elapsed:.2f} seconds.")
    
    # --- 5. Generate Stats ---
    cursor.execute("SELECT count(*) FROM asddo_voters WHERE json_extract(data, '$.ASDDO_14_Aug') = 'Yes'")
    yes_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT count(*) FROM asddo_voters WHERE json_extract(data, '$.ASDDO_14_Aug') = 'No'")
    no_count = cursor.fetchone()[0]

    cursor.execute("SELECT count(*) FROM asddo_voters WHERE json_extract(data, '$.ASDDO_14_Aug') = 'NYU'")
    nyu_count = cursor.fetchone()[0]
    
    print(f"\n📊 RESULTS FOR AUGUST 14TH:")
    print(f"🟢 Still on the list (Yes): {yes_count:,}")
    print(f"🔴 Successfully removed (No): {no_count:,}")
    print(f"⏳ Not Yet Updated by ECI (NYU): {nyu_count:,}")
    print(f"✨ Brand new voters secretly added: {new_inserts:,}")
    
    print("\nNext step: Push the delta updates to TiDB to sync the live website!")
    
    cursor.execute("DROP TABLE temp_aug14_epics")
    cursor.execute("DROP TABLE temp_skipped_booths")
    conn.commit()
    conn.close()

if __name__ == '__main__':
    sync_local_data()

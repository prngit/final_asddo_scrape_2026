import os
import zipfile
import re
import pdfplumber
import pymysql
import concurrent.futures
import io

HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
PORT = 4000
USER = "ktQaY5Y9x7TFRvV.root"
PASSWORD = "qUlM80c5MitEvh2e"
DATABASE = "test"

ZIP_DIR = r"D:\SIR_2026_ASDDO\Districtwise_ASDDO_5th6thAugust"
BATCH_SIZE = 2000

def get_db_connection():
    return pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        ssl={"rejectUnauthorized": True}
    )

def extract_metadata(zip_filename, pdf_filename):
    district = "Unknown"
    
    overrides = {
        "36": "Yadgiri", "37": "Yadgiri", "38": "Yadgiri", "39": "Yadgiri",
        "49": "Bidar",
        "64": "Koppala",
        "98": "Chitradurga",
        "130": "Tumakuru", "131": "Tumakuru",
        "KUDLIGI": "Vijayanagara"
    }
    
    zip_upper = zip_filename.upper()
    for key, val in overrides.items():
        if key in zip_upper:
            district = val
            break
            
    if district == "Unknown":
        parts = zip_filename.split('-')
        if len(parts) >= 2:
            district = parts[0].strip()

    filename = os.path.basename(pdf_filename)
    parts = filename.split('-')
    
    ac_name = "Unknown"
    for p in filename.split('/'):
        if '-' in p and p.split('-')[0].isdigit():
            ac_name = p.strip()
            break
            
    if ac_name == "Unknown" and len(parts) >= 2:
        ac_name = parts[1] 

    if len(parts) >= 4:
        return district, ac_name, parts[2], parts[3]
    return district, ac_name, "Unknown", "Unknown"

def process_pdf(pdf_stream, district):
    records = []
    try:
        with pdfplumber.open(pdf_stream) as pdf:
            ac_no = "Unknown"
            part_no = "Unknown"
            
            if len(pdf.pages) > 0:
                first_page_text = pdf.pages[0].extract_text()
                if first_page_text:
                    ac_match = re.search(r'AC:\s*(.*?)\s*;\s*Part:\s*(.*)', first_page_text, re.IGNORECASE)
                    if ac_match:
                        ac_no = ac_match.group(1).strip()
                        part_no = ac_match.group(2).strip()

            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if not tables:
                    continue
                for table in tables:
                    for row in table:
                        if len(row) < 4: 
                            continue
                        raw_row = [str(cell) if cell else "" for cell in row]
                        cleaned_row = [str(cell).replace('\n', ' ').strip() if cell else "" for cell in row]
                        
                        asddo_serial_str = cleaned_row[0]
                        serial_str = cleaned_row[1]
                        epic_str = cleaned_row[2][:50]
                        name_str = cleaned_row[3][:255]
                        
                        uncollectable_reason = None
                        relative_details = None
                        if len(cleaned_row) >= 5:
                            relative_details = cleaned_row[4][:255]
                            if not relative_details or relative_details.isspace():
                                relative_details = None
                                
                        if not relative_details:
                            if len(raw_row) >= 4 and '(' in raw_row[3]:
                                text = raw_row[3]
                                if '(Father)' in text or '(Husband)' in text or '(Mother)' in text or '(Wife)' in text or '(Other)' in text:
                                    parts = text.split('\n')
                                    if len(parts) > 1:
                                        name_str = parts[0].replace('\n', ' ').strip()[:255]
                                        relative_details = parts[1].replace('\n', ' ').strip()[:255]
                                    else:
                                        match = re.search(r'(.*?)\s+([^\s]+\s*\((?:Father|Husband|Mother|Wife|Other)\))', text, re.IGNORECASE)
                                        if match:
                                            name_str = match.group(1).replace('\n', ' ').strip()[:255]
                                            relative_details = match.group(2).replace('\n', ' ').strip()[:255]
                                        
                            if not relative_details and len(raw_row) >= 6:
                                text = raw_row[5]
                                if '(Father)' in text or '(Husband)' in text or '(Mother)' in text or '(Wife)' in text or '(Other)' in text:
                                    parts = text.split('\n')
                                    for p in parts:
                                        if '(' in p and ')' in p and not p.strip().startswith('(') or ('Father' in p or 'Husband' in p or 'Mother' in p or 'Wife' in p):
                                            relative_details = p.replace('\n', ' ').strip()[:255]
                                            
                        if len(cleaned_row) >= 7:
                            uncollectable_reason = cleaned_row[6][:100]
                            if not uncollectable_reason or uncollectable_reason.isspace():
                                uncollectable_reason = None
                        
                        if re.search(r'([A-Z]{3}\d{7})', epic_str):
                            try:
                                serial_no = int(serial_str)
                            except ValueError:
                                serial_no = 0
                                
                            try:
                                asddo_serial_no = int(asddo_serial_str)
                            except ValueError:
                                asddo_serial_no = 0
                            
                            records.append((
                                epic_str, 
                                name_str, 
                                district[:100], 
                                ac_no[:255], 
                                part_no[:255],
                                asddo_serial_no,
                                serial_no, 
                                uncollectable_reason,
                                relative_details
                            ))
    except Exception as e:
        pass
    return records

def insert_batch(cursor, records):
    sql = """
    INSERT IGNORE INTO asddo_voters 
    (epic_no, voter_name, district, ac_no, part_no, asddo_serial_no, serial_no, uncollectable_reason, relative_details)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(sql, records)

def worker_process_pdf(zip_path, pdf_name, district):
    try:
        with zipfile.ZipFile(zip_path) as z:
            with z.open(pdf_name) as f:
                pdf_bytes = f.read()
        pdf_stream = io.BytesIO(pdf_bytes)
        return process_pdf(pdf_stream, district)
    except Exception as e:
        return []

def process_zip(zip_filename):
    zip_path = os.path.join(ZIP_DIR, zip_filename)
    print(f"\nStarting ZIP: {zip_filename}", flush=True)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    total_records = 0
    batch = []
    
    try:
        with zipfile.ZipFile(zip_path) as z:
            pdf_files = [f for f in z.namelist() if f.endswith('.pdf')]
            print(f"[{zip_filename}] Found {len(pdf_files)} PDFs", flush=True)
            
        district = extract_metadata(zip_filename, pdf_files[0] if pdf_files else "")[0]
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            futures = []
            for pdf_name in pdf_files:
                futures.append(executor.submit(worker_process_pdf, zip_path, pdf_name, district))
                
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                if i % 50 == 0 and i > 0:
                    print(f"[{zip_filename}] Processed {i}/{len(pdf_files)} PDFs...", flush=True)
                    
                records = future.result()
                if records:
                    batch.extend(records)
                    
                if len(batch) >= BATCH_SIZE:
                    insert_batch(cursor, batch)
                    conn.commit()
                    total_records += len(batch)
                    batch = []
                    
        # Insert remaining
        if batch:
            insert_batch(cursor, batch)
            conn.commit()
            total_records += len(batch)
            
        print(f"[{zip_filename}] DONE! Inserted {total_records} voter records.", flush=True)
    except Exception as e:
        print(f"Error reading ZIP {zip_filename}: {e}", flush=True)
    finally:
        cursor.close()
        conn.close()

def main():
    zip_files = [f for f in os.listdir(ZIP_DIR) if f.endswith('.zip')]
    print(f"Found {len(zip_files)} ZIP files to process.", flush=True)
    
    if not zip_files:
        print("No zip files found in the directory. Exiting.")
        return

    print("Starting SUPERCHARGED Multiprocessing Harvester...", flush=True)
    
    for zip_file in zip_files:
        process_zip(zip_file)
        
    print("\nALL ZIPS COMPLETED SUCCESSFULLY!", flush=True)

if __name__ == "__main__":
    main()

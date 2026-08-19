import { execSync, spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import mysql from 'mysql2/promise';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SCRAPER_DIR = "F:\\Prashanth\\2002Voterlist\\karnataka-asddo-dashboard-main";
const CACHE_DIR = path.join(SCRAPER_DIR, "cache");
const EXTRACTED_DIR = path.join(CACHE_DIR, "extracted");

const host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com";
const port = 4000;
const user = "ktQaY5Y9x7TFRvV.root";
const password = "qUlM80c5MitEvh2e";
const database = "test"; 

function log(msg) {
    console.log(`[SYNC] ${new Date().toISOString()} - ${msg}`);
}

async function runCommand(cmd, cwd, args = []) {
    return new Promise((resolve, reject) => {
        const proc = spawn(cmd, args, { cwd, shell: true, stdio: 'inherit' });
        proc.on('close', (code) => {
            if (code === 0) resolve();
            else reject(new Error(`Command ${cmd} failed with exit code ${code}`));
        });
    });
}

async function main() {
    log("Starting Continuous Incremental Sync...");
    
    let connection;
    try {
        connection = await mysql.createConnection({
            host, port, user, password, database,
            ssl: { rejectUnauthorized: true }
        });
        
        // 1. Discover latest Drive structure
        log("Running Discovery Phase...");
        await runCommand("npm", SCRAPER_DIR, ["run", "discover"]);
        
        // 2. Read Manifest and DB state
        log("Reading manifest and identifying new files...");
        const manifestPath = path.join(CACHE_DIR, 'manifest.json');
        if (!fs.existsSync(manifestPath)) throw new Error("Manifest not found.");
        const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
        
        const [rows] = await connection.execute("SELECT file_id FROM processed_drive_files");
        const processedIds = new Set(rows.map(r => r.file_id));
        
        let newFilesFound = false;
        
        // 3. Prepare the Extraction Directory
        // To ensure we ONLY import new rows, we clear the extracted directory 
        // but pre-populate the .done files with the known IDs so the scraper skips them.
        if (fs.existsSync(EXTRACTED_DIR)) {
            fs.rmSync(EXTRACTED_DIR, { recursive: true, force: true });
        }
        fs.mkdirSync(EXTRACTED_DIR, { recursive: true });
        
        const slug = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
        
        for (const district of manifest.districts) {
            let hasNewInDistrict = false;
            let doneContent = "";
            for (const ac of district.acs) {
                for (const file of ac.files) {
                    const key = file.zipId || file.zipUrl ? `${file.zipId ?? file.zipUrl}#${file.entry}` : (file.id ?? file.url);
                    if (!key) continue;
                    
                    if (processedIds.has(key)) {
                        doneContent += key + "\n";
                    } else {
                        newFilesFound = true;
                        hasNewInDistrict = true;
                    }
                }
            }
            // Pre-seed the .done file for this district so it skips old files
            fs.writeFileSync(path.join(EXTRACTED_DIR, `${slug(district.name)}.done`), doneContent);
        }
        
        if (!newFilesFound) {
            log("No new PDF updates found on the CEO website. Sync complete.");
            return;
        }
        
        log("New updates found! Running Extraction Phase (this will only download new files)...");
        // Concurrency 8 as requested
        await runCommand("node", SCRAPER_DIR, ["scripts/2-extract.mjs", "--concurrency", "8"]);
        
        // 4. Import the newly extracted NDJSON files to TiDB
        log("Extraction complete. Importing new data to TiDB...");
        const todayDate = new Date().toISOString().split('T')[0];
        
        // Use our existing import script
        await runCommand("node", __dirname, ["import_ndjson_to_tidb.js", todayDate, EXTRACTED_DIR]);
        
        // 5. Update the tracking table with the newly processed files
        log("Updating internal tracking table...");
        const doneFiles = fs.readdirSync(EXTRACTED_DIR).filter(f => f.endsWith('.done'));
        const newIdsToTrack = [];
        for (const df of doneFiles) {
            const content = fs.readFileSync(path.join(EXTRACTED_DIR, df), 'utf8');
            const lines = content.split('\n').filter(Boolean);
            for (const line of lines) {
                if (!processedIds.has(line)) {
                    newIdsToTrack.push([line, todayDate]);
                }
            }
        }
        
        if (newIdsToTrack.length > 0) {
            // Batch insert the new tracking IDs
            const batchSize = 1000;
            for (let i = 0; i < newIdsToTrack.length; i += batchSize) {
                const batch = newIdsToTrack.slice(i, i + batchSize);
                await connection.query("INSERT IGNORE INTO processed_drive_files (file_id, processed_date) VALUES ?", [batch]);
            }
            log(`Tracked ${newIdsToTrack.length} new files.`);
        }
        
        log("Continuous Sync Process Completed Successfully!");
        
    } catch (error) {
        log(`Sync Failed: ${error.message}`);
    } finally {
        if (connection) await connection.end();
    }
}

main();

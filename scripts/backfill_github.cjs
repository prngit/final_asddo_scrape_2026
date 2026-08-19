const sqlite3 = require('sqlite3').verbose();
const crypto = require('crypto');

const DB_PATH = 'D:/SRI WebApp/master.db';
const GITHUB_BASE_URL = 'https://raw.githubusercontent.com/gouthamganeshm/karnataka-asddo-dashboard/main/docs/data/asddo';
const CATEGORIES = ['absent', 'shifted', 'death', 'duplicate', 'others'];

function sha256hex(s) {
    return crypto.createHash('sha256').update(s, 'utf8').digest('hex');
}

async function run() {
    console.log("🚀 Starting GitHub Data Backfill Pipeline...");
    const db = new sqlite3.Database(DB_PATH);

    console.log("🔍 Scanning local SQLite database for missing voter details...");
    const rows = await new Promise((resolve, reject) => {
        db.all(`SELECT id, epic_no, data FROM asddo_voters WHERE voter_name IS NULL`, (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });

    console.log(`Found ${rows.length} voters missing details.`);
    if (rows.length === 0) return;

    const shards = new Map();
    for (const r of rows) {
        const hash = sha256hex(r.epic_no);
        const folder = hash.substring(0, 2);
        const file = hash.substring(2, 4);
        const hash8 = hash.substring(4, 12);
        const urlPath = `${folder}/${file}.json`;
        
        if (!shards.has(urlPath)) shards.set(urlPath, []);
        shards.get(urlPath).push({ ...r, hash8 });
    }

    console.log(`Grouped into ${shards.size} unique GitHub shards.`);

    let processedShards = 0;
    const shardEntries = Array.from(shards.entries());
    
    // Thread-safe collection for SQLite updates to prevent SQLITE_BUSY locks
    const updates = [];

    async function processShard(urlPath, voters) {
        try {
            const res = await fetch(`${GITHUB_BASE_URL}/${urlPath}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            for (const v of voters) {
                const match = data.find(arr => arr[0] === v.hash8);
                if (match) {
                    const voterName = match[1] || '';
                    const relativeName = match[2] || '';
                    const age = match[4] || 0;
                    const serialNo = match[5] || 0;
                    const reasonIdx = match[6];
                    const reason = (reasonIdx !== undefined && CATEGORIES[reasonIdx]) ? CATEGORIES[reasonIdx] : '';

                    let jsonPayload;
                    try { jsonPayload = JSON.parse(v.data || "{}"); } catch(e) { jsonPayload = {}; }
                    jsonPayload.age = age;
                    if (reason) jsonPayload.github_reason = reason;

                    updates.push({
                        voterName, relativeName, serialNo, reason: reason || null, 
                        jsonStr: JSON.stringify(jsonPayload), id: v.id
                    });
                }
            }
        } catch (e) {
            // Ignore 404s (shard might just not exist)
        }
        processedShards++;
        if (processedShards % 100 === 0) {
            console.log(`✅ Fetched ${processedShards} / ${shards.size} shards... Found ${updates.length} matches so far.`);
        }
    }

    const CONCURRENCY = 40;
    let i = 0;
    const workers = Array(CONCURRENCY).fill(0).map(async () => {
        while (i < shardEntries.length) {
            const [urlPath, voters] = shardEntries[i++];
            await processShard(urlPath, voters);
        }
    });

    await Promise.all(workers);

    console.log(`\n✅ Finished fetching! Found details for ${updates.length} / ${rows.length} voters.`);
    console.log(`💾 Beginning bulk SQLite injection...`);
    
    await new Promise(r => db.run("BEGIN TRANSACTION", r));
    
    let injected = 0;
    const stmt = db.prepare(`UPDATE asddo_voters SET 
        voter_name = ?, relative_details = ?, serial_no = ?, 
        uncollectable_reason = COALESCE(uncollectable_reason, ?), data = ? 
        WHERE id = ?`);

    for (const u of updates) {
        await new Promise((resolve, reject) => {
            stmt.run([u.voterName, u.relativeName, u.serialNo, u.reason, u.jsonStr, u.id], (err) => {
                if (err) reject(err); else resolve();
            });
        });
        injected++;
        if (injected % 10000 === 0) console.log(`💉 Injected ${injected} / ${updates.length}`);
    }

    await new Promise(r => stmt.finalize(r));
    await new Promise(r => db.run("COMMIT", r));
    
    console.log("🎉 Completely finished backfilling local master.db!");
}

run().catch(console.error);

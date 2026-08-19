require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const mysql = require('mysql2/promise');
const path = require('path');

const SQLITE_DB = path.join(__dirname, '../master.db');

async function main() {
    console.log("🚀 Starting Optimized Delta Push to TiDB...");

    // Connect to local SQLite
    console.log("🔌 Connecting to SQLite master.db...");
    const db = new sqlite3.Database(SQLITE_DB, sqlite3.OPEN_READONLY);

    // Connect to TiDB
    console.log("🔌 Connecting to TiDB Cloud...");
    const tidbConnection = await mysql.createPool({
        uri: process.env.TIDB_URI,
        ssl: { rejectUnauthorized: true },
        waitForConnections: true,
        connectionLimit: 4,
        queueLimit: 0,
        enableKeepAlive: true,
        keepAliveInitialDelay: 10000
    });

    console.log("✅ TiDB Connected!");

    // Query the delta: anyone removed (ASDDO_11_Aug = No) OR newly added (ASDDO_03to10_Aug = No)
    console.log("🔍 Finding delta updates in SQLite...");
    
    const BATCH_SIZE = 2500;
    
    const query = `
        SELECT data
        FROM asddo_voters
        WHERE json_extract(data, '$.age') IS NOT NULL
          AND json_extract(data, '$.epic') IS NOT NULL
    `;

    const flushBatch = async (batch, attempt = 1) => {
        if (batch.length === 0) return;
        try {
            const sql = `
                INSERT INTO asddo_voters (data)
                VALUES ?
                ON DUPLICATE KEY UPDATE data = VALUES(data)
            `;
            await tidbConnection.query(sql, [batch]);
        } catch (error) {
            console.error(`\n❌ TiDB Batch Insert Error (Attempt ${attempt}):`, error.message);
            if (attempt <= 5) {
                console.log("⏳ Retrying in 5 seconds...");
                await new Promise(r => setTimeout(r, 5000));
                return flushBatch(batch, attempt + 1);
            }
            throw error;
        }
    };

    const CHUNK_SIZE = 50000;
    let offset = 0;
    let totalProcessed = 0;
    const CONCURRENCY = 4;
    
    console.log(`⚡ Igniting ${CONCURRENCY} parallel engines to swarm TiDB with the 759k Newly Enriched Voters...`);

    while (true) {
        const chunk = await new Promise((resolve, reject) => {
            db.all(`${query} LIMIT ${CHUNK_SIZE} OFFSET ${offset}`, [], (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });

        if (chunk.length === 0) break;

        // Group rows into small TiDB batches
        const batches = [];
        let currentBatch = [];
        for (const row of chunk) {
            currentBatch.push([row.data]);
            if (currentBatch.length >= BATCH_SIZE) {
                batches.push(currentBatch);
                currentBatch = [];
            }
        }
        if (currentBatch.length > 0) batches.push(currentBatch);

        let currentIndex = 0;
        
        async function worker() {
            while (currentIndex < batches.length) {
                const batch = batches[currentIndex++];
                await flushBatch(batch);
                totalProcessed += batch.length;
                process.stdout.write(`\r🚀 Supercharging... Pushed ${totalProcessed} / ~3,996,752 delta rows to TiDB...`);
            }
        }

        const workers = Array.from({length: CONCURRENCY}, () => worker());
        await Promise.all(workers);
        
        offset += CHUNK_SIZE;
    }

    console.log(`\n🎉 Push Complete! Safely pushed ${totalProcessed} rows to TiDB.`);
    await tidbConnection.end();
    db.close();
    process.exit(0);
}

main().catch(console.error);

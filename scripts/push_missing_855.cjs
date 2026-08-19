require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const mysql = require('mysql2/promise');
const path = require('path');

const SQLITE_DB = path.join(__dirname, '../master.db');

async function main() {
    console.log("🚀 Starting Push for Missing 855 Voters...");
    
    // Connect to SQLite
    const sqliteDb = new sqlite3.Database(SQLITE_DB);
    
    // Connect to TiDB
    const tidbConn = await mysql.createConnection({
        uri: process.env.TIDB_URI,
        ssl: {
            rejectUnauthorized: true
        }
    });
    console.log("✅ TiDB Connected!");

    // Query 855 voters from SQLite
    const rows = await new Promise((resolve, reject) => {
        sqliteDb.all(`
            SELECT *
            FROM asddo_voters
            WHERE json_extract(data, '$.ASDDO_03to10_Aug') = 'No' 
              AND json_extract(data, '$.ASDDO_11_Aug') = 'Yes'
        `, (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });

    console.log(`🔍 Found ${rows.length} missing voters in SQLite.`);

    // Push to TiDB
    if (rows.length > 0) {
        let values = rows.map(r => {
            // We MUST reconstruct the full JSON because TiDB uses generated columns!
            const statusJson = JSON.parse(r.data);
            const fullJson = {
                epic: r.epic_no,
                district: r.district,
                acNo: String(r.ac_no || ''),
                partNo: Number(r.part_no) || 0,
                slno: Number(r.serial_no) || 0,
                name: r.voter_name,
                relativeName: r.relative_details,
                reasonRaw: r.uncollectable_reason,
                ...statusJson
            };
            return [JSON.stringify(fullJson)]; // ONLY insert data
        });

        // Batch insert
        const BATCH_SIZE = 500;
        let pushed = 0;
        for (let i = 0; i < values.length; i += BATCH_SIZE) {
            const batch = values.slice(i, i + BATCH_SIZE);
            await tidbConn.query(`
                INSERT INTO asddo_voters (data)
                VALUES ?
                ON DUPLICATE KEY UPDATE data=VALUES(data)
            `, [batch]);
            pushed += batch.length;
            console.log(`✅ Pushed ${pushed} / ${values.length}`);
        }
    }
    
    await tidbConn.end();
    console.log("🎉 Complete!");
}

main().catch(err => {
    console.error("MySQL Error Code:", err.code);
    console.error("MySQL Error Message:", err.message);
});

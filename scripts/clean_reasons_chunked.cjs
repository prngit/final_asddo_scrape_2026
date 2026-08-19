require('dotenv').config();
const mysql = require('mysql2/promise');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const SQLITE_DB = path.join(__dirname, '../master.db');

async function cleanTiDB() {
  console.log("🔌 Connecting to TiDB Cloud...");
  const tidb = await mysql.createConnection({
    uri: process.env.TIDB_URI,
    ssl: { rejectUnauthorized: true }
  });

  const [minMax] = await tidb.query("SELECT MIN(id) as min_id, MAX(id) as max_id FROM asddo_voters");
  const minId = minMax[0].min_id || 1;
  const maxId = minMax[0].max_id || 26000000;
  console.log(`📊 Processing TiDB IDs from ${minId} to ${maxId}...`);

  const CHUNK_SIZE = 100000;
  let totalCleaned = 0;

  for (let start = minId; start <= maxId; start += CHUNK_SIZE) {
    const end = start + CHUNK_SIZE - 1;
    const [res] = await tidb.query(`
      UPDATE asddo_voters 
      SET data = JSON_REMOVE(data, '$.reasonRaw') 
      WHERE id BETWEEN ? AND ?
        AND (
          uncollectable_reason REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}'
          OR uncollectable_reason REGEXP '^\\\\([0-9]+\\\\)$'
          OR uncollectable_reason REGEXP '^[0-9]+$'
          OR uncollectable_reason = 'null'
          OR uncollectable_reason = 'undefined'
        )
    `, [start, end]);

    totalCleaned += res.affectedRows;
    process.stdout.write(`\r⏳ Checked IDs ${start} - ${end}... (Cleaned so far: ${totalCleaned} rows)`);
  }

  console.log(`\n✅ TiDB reason normalization complete! Cleaned ${totalCleaned} rows.`);
  await tidb.end();
}

async function cleanSQLite() {
  console.log("🔌 Connecting to SQLite master.db...");
  const db = new sqlite3.Database(SQLITE_DB);

  console.log("🧹 Normalizing reasons in local SQLite master.db...");
  
  await new Promise((resolve, reject) => {
    db.run(`
      UPDATE asddo_voters
      SET uncollectable_reason = NULL,
          data = json_remove(data, '$.reasonRaw')
      WHERE uncollectable_reason LIKE '%/__/____%'
         OR uncollectable_reason LIKE '(%)'
         OR uncollectable_reason = 'null'
         OR uncollectable_reason = 'undefined'
    `, (err) => {
      if (err) reject(err);
      else {
        console.log("✅ Local SQLite database reasons normalized.");
        resolve();
      }
    });
  });

  db.close();
}

async function main() {
  await cleanTiDB();
  await cleanSQLite();
  console.log("🎉 Reason normalization complete across all databases!");
}

main().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});

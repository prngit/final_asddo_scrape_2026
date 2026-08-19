require('dotenv').config();
const mysql = require('mysql2/promise');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const SQLITE_DB = path.join(__dirname, '../master.db');

async function cleanTiDB() {
  console.log("🔌 Connecting to TiDB...");
  const tidb = await mysql.createConnection({
    uri: process.env.TIDB_URI,
    ssl: { rejectUnauthorized: true }
  });

  console.log("🧹 Cleaning date-formatted reasons (e.g. DD/MM/YYYY)...");
  const [res1] = await tidb.query(`
    UPDATE asddo_voters 
    SET data = JSON_REMOVE(data, '$.reasonRaw') 
    WHERE uncollectable_reason REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}'
  `);
  console.log(`✅ Removed date strings from ${res1.affectedRows} rows in TiDB.`);

  console.log("🧹 Cleaning age-in-parentheses reasons (e.g. (42))...");
  const [res2] = await tidb.query(`
    UPDATE asddo_voters 
    SET data = JSON_REMOVE(data, '$.reasonRaw') 
    WHERE uncollectable_reason REGEXP '^\\\\([0-9]+\\\\)$' OR uncollectable_reason REGEXP '^[0-9]+$'
  `);
  console.log(`✅ Removed age numbers from ${res2.affectedRows} rows in TiDB.`);

  console.log("🧹 Cleaning literal 'null' string reasons...");
  const [res3] = await tidb.query(`
    UPDATE asddo_voters 
    SET data = JSON_REMOVE(data, '$.reasonRaw') 
    WHERE uncollectable_reason = 'null' OR uncollectable_reason = 'undefined'
  `);
  console.log(`✅ Cleaned literal nulls from ${res3.affectedRows} rows in TiDB.`);

  await tidb.end();
}

async function cleanSQLite() {
  console.log("🔌 Connecting to SQLite master.db...");
  const db = new sqlite3.Database(SQLITE_DB);

  console.log("🧹 Cleaning messy reasons in local master.db...");
  
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
        console.log("✅ Local SQLite database reasons cleaned.");
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
  console.error("❌ Error cleaning reasons:", err);
  process.exit(1);
});

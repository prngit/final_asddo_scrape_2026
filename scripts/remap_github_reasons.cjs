require('dotenv').config();
const crypto = require('crypto');
const https = require('https');
const sqlite3 = require('sqlite3').verbose();
const mysql = require('mysql2/promise');
const path = require('path');

const SQLITE_DB = path.join(__dirname, '../master.db');

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        return resolve(null);
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          resolve(null);
        }
      });
    }).on('error', (err) => resolve(null));
  });
}

function cleanReason(raw) {
  if (!raw || raw === 'Not stated') return null;
  const s = raw.trim();
  if (/^Permanently/i.test(s)) return 'Permanently Shifted';
  if (/^Untraceable/i.test(s)) return 'Untraceable/Absent';
  if (/^Death/i.test(s)) return 'Death';
  if (/^Already enrolled/i.test(s) || /Already Enrolled/i.test(s)) return 'Duplicate';
  if (/^EF Refused/i.test(s)) return 'EF Refused';
  return s;
}

function cleanRelation(raw) {
  if (!raw) return '';
  const s = raw.replace(/\0/g, '').trim();
  if (/^Father/i.test(s)) return 'Father';
  if (/^Husband/i.test(s)) return 'Husband';
  if (/^Mother/i.test(s)) return 'Mother';
  if (/^Wife/i.test(s)) return 'Wife';
  if (/^Other/i.test(s)) return 'Other';
  if (/^GUARDIAN/i.test(s)) return 'Guardian';
  return s;
}

async function main() {
  console.log("📥 Fetching official manifest.json dictionary from GitHub...");
  const manifest = await fetchJson('https://raw.githubusercontent.com/gouthamganeshm/karnataka-asddo-dashboard/main/docs/data/manifest.json');
  if (!manifest || !manifest.dicts) {
    throw new Error("Failed to load manifest.json");
  }
  const dictReasons = manifest.dicts.reasons;
  const dictRelations = manifest.dicts.relations;
  console.log(`✅ Loaded ${dictReasons.length} reason entries and ${dictRelations.length} relation entries.`);

  console.log("🔌 Connecting to SQLite master.db...");
  const db = new sqlite3.Database(SQLITE_DB);

  console.log("🔍 Finding backfilled records needing exact reason & relative mapping...");
  const rows = await new Promise((resolve, reject) => {
    db.all(`
      SELECT id, epic_no, voter_name, relative_details, uncollectable_reason, data
      FROM asddo_voters
      WHERE json_extract(data, '$.github_reason') IS NOT NULL
    `, [], (err, res) => {
      if (err) reject(err);
      else resolve(res);
    });
  });

  console.log(`📋 Found ${rows.length} records. Grouping by GitHub shard...`);

  // Group by shard prefix (first 2 hex chars / next 2 hex chars)
  const shardMap = new Map();
  for (const row of rows) {
    const hash = crypto.createHash('sha256').update(row.epic_no).digest('hex');
    const folder = hash.substring(0, 2);
    const file = hash.substring(2, 4);
    const hash8 = hash.substring(4, 12);
    const shardKey = `${folder}/${file}`;

    if (!shardMap.has(shardKey)) shardMap.set(shardKey, []);
    shardMap.get(shardKey).push({ id: row.id, epic_no: row.epic_no, hash8, row });
  }

  console.log(`🗂️ Distributed across ${shardMap.size} unique shard files.`);

  const shardKeys = Array.from(shardMap.keys());
  const CONCURRENCY = 25;
  let shardIndex = 0;
  let updatedCount = 0;

  db.run("BEGIN TRANSACTION");
  const updateStmt = db.prepare(`
    UPDATE asddo_voters
    SET relative_details = ?, uncollectable_reason = ?, data = ?
    WHERE id = ?
  `);

  async function worker() {
    while (shardIndex < shardKeys.length) {
      const key = shardKeys[shardIndex++];
      const items = shardMap.get(key);
      const url = `https://raw.githubusercontent.com/gouthamganeshm/karnataka-asddo-dashboard/main/docs/data/asddo/${key}.json`;
      
      const shardData = await fetchJson(url);
      if (!shardData) continue;

      const recordMap = new Map();
      for (const rec of shardData) {
        recordMap.set(rec[0], rec);
      }

      for (const item of items) {
        const matched = recordMap.get(item.hash8);
        if (matched) {
          const rawRelativeName = matched[2] || '';
          const relIdx = matched[3];
          const rawRelName = relIdx >= 0 && dictRelations[relIdx] ? cleanRelation(dictRelations[relIdx]) : '';
          
          const relativeDetails = rawRelName ? `${rawRelativeName} (${rawRelName})` : rawRelativeName;
          
          const reasonIdx = matched[6];
          const rawReason = reasonIdx >= 0 && dictReasons[reasonIdx] ? cleanReason(dictReasons[reasonIdx]) : null;

          let payload;
          try {
            payload = typeof item.row.data === 'string' ? JSON.parse(item.row.data) : (item.row.data || {});
          } catch(e) {
            payload = {};
          }

          payload.relativeName = relativeDetails;
          payload.reasonRaw = rawReason;
          if (rawReason) {
            payload.github_reason = rawReason.toLowerCase();
          } else {
            delete payload.github_reason;
          }

          const updatedJson = JSON.stringify(payload);
          updateStmt.run([relativeDetails, rawReason, updatedJson, item.id]);
          updatedCount++;
        }
      }

      if (shardIndex % 50 === 0 || shardIndex === shardKeys.length) {
        process.stdout.write(`\r⏳ Fetched ${shardIndex}/${shardKeys.length} shards... Updated ${updatedCount} records`);
      }
    }
  }

  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  await Promise.all(workers);

  console.log("\n💾 Committing SQLite updates...");
  await new Promise(resolve => updateStmt.finalize(resolve));
  await new Promise(resolve => db.run("COMMIT", resolve));
  db.close();

  console.log("✅ Local SQLite database updated with exact PDF reasons and relations!");

  // Now push updated backfilled records to TiDB
  console.log("🚀 Syncing updated reasons to TiDB Cloud...");
  const pushCmd = require('child_process').spawn('node', ['scripts/push_to_tidb.cjs'], { stdio: 'inherit' });
  pushCmd.on('close', (code) => {
    console.log(`🎉 Full reconciliation completed with exit code ${code}!`);
    process.exit(code);
  });
}

main().catch(err => {
  console.error("❌ Error in remapping:", err);
  process.exit(1);
});

require('dotenv').config();
const crypto = require('crypto');
const https = require('https');
const sqlite3 = require('sqlite3').verbose();
const mysql = require('mysql2/promise');
const path = require('path');

const SQLITE_DB = path.join(__dirname, '../master.db');

function fetchJson(url) {
  return new Promise((resolve) => {
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
    }).on('error', () => resolve(null));
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
  console.log("🚀 Starting Full State-Wide August 14th ASDDO Status Reconciliation...");

  // 1. Fetch manifest and dictionaries
  console.log("📥 Loading manifest.json and dictionaries from GitHub CDN...");
  const manifest = await fetchJson('https://raw.githubusercontent.com/gouthamganeshm/karnataka-asddo-dashboard/main/docs/data/manifest.json');
  if (!manifest || !manifest.dicts) {
    throw new Error("Failed to load manifest.json from CDN");
  }
  const dictReasons = manifest.dicts.reasons;
  const dictRelations = manifest.dicts.relations;
  console.log(`✅ Loaded ${dictReasons.length} reason categories and ${dictRelations.length} relation categories.`);

  // 2. Connect to SQLite
  console.log("🔌 Connecting to SQLite master.db...");
  const db = new sqlite3.Database(SQLITE_DB);

  console.log("🔍 Loading all records from master.db to group by shard...");
  const rows = await new Promise((resolve, reject) => {
    db.all(`
      SELECT id, epic_no, voter_name, relative_details, uncollectable_reason, data
      FROM asddo_voters
      WHERE epic_no IS NOT NULL
    `, [], (err, res) => {
      if (err) reject(err);
      else resolve(res);
    });
  });

  console.log(`📋 Total records loaded: ${rows.length.toLocaleString()}`);

  // Group all local records by shard file: `${folder}/${file}`
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

  console.log(`🗂️ Distributed across ${shardMap.size.toLocaleString()} unique shard buckets.`);

  const shardKeys = Array.from(shardMap.keys());
  const CONCURRENCY = 30;
  let shardIndex = 0;

  let totalActiveAug14 = 0;
  let totalRemovedAug14 = 0;

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
      const recordMap = new Map();
      if (shardData) {
        for (const rec of shardData) {
          recordMap.set(rec[0], rec);
        }
      }

      for (const item of items) {
        const matched = recordMap.get(item.hash8);

        let payload;
        try {
          payload = typeof item.row.data === 'string' ? JSON.parse(item.row.data) : (item.row.data || {});
        } catch (e) {
          payload = {};
        }

        let updatedRelativeDetails = item.row.relative_details;
        let updatedReason = item.row.uncollectable_reason;

        if (matched) {
          // Voter IS on August 14 ASDDO list
          totalActiveAug14++;
          payload.ASDDO_14_Aug = 'Yes';

          const rawRelativeName = matched[2] || '';
          const relIdx = matched[3];
          const rawRelName = relIdx >= 0 && dictRelations[relIdx] ? cleanRelation(dictRelations[relIdx]) : '';
          updatedRelativeDetails = rawRelName ? `${rawRelativeName} (${rawRelName})` : (rawRelativeName || item.row.relative_details);

          const reasonIdx = matched[6];
          const rawReason = reasonIdx >= 0 && dictReasons[reasonIdx] ? cleanReason(dictReasons[reasonIdx]) : null;
          updatedReason = rawReason || item.row.uncollectable_reason;

          payload.relativeName = updatedRelativeDetails;
          payload.reasonRaw = updatedReason;
          if (updatedReason) {
            payload.github_reason = updatedReason.toLowerCase();
          }
        } else {
          // Voter is NOT on August 14 ASDDO list (removed)
          totalRemovedAug14++;
          payload.ASDDO_14_Aug = 'No';
        }

        // Ensure baseline flags exist
        if (!payload.ASDDO_03to10_Aug) {
          payload.ASDDO_03to10_Aug = 'Yes';
        }
        if (!payload.ASDDO_11_Aug) {
          payload.ASDDO_11_Aug = payload.ASDDO_03to10_Aug;
        }

        const updatedJson = JSON.stringify(payload);
        updateStmt.run([updatedRelativeDetails, updatedReason, updatedJson, item.id]);
      }

      if (shardIndex % 100 === 0 || shardIndex === shardKeys.length) {
        process.stdout.write(`\r⏳ Reconciled ${shardIndex.toLocaleString()}/${shardKeys.length.toLocaleString()} shards... (Active Aug 14: ${totalActiveAug14.toLocaleString()}, Removed: ${totalRemovedAug14.toLocaleString()})`);
      }
    }
  }

  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  await Promise.all(workers);

  console.log("\n💾 Committing SQLite transaction to master.db...");
  await new Promise(resolve => updateStmt.finalize(resolve));
  await new Promise(resolve => db.run("COMMIT", resolve));
  db.close();

  console.log(`\n🎉 Reconciliation completed locally!`);
  console.log(`  🟢 Active on August 14 ASDDO List: ${totalActiveAug14.toLocaleString()}`);
  console.log(`  🔴 Successfully Removed from List: ${totalRemovedAug14.toLocaleString()}`);

  console.log("\n🚀 Launching sync to push reconciled state to TiDB Cloud...");
  const pushCmd = require('child_process').spawn('node', ['scripts/push_to_tidb.cjs'], { stdio: 'inherit' });
  pushCmd.on('close', (code) => {
    console.log(`🎉 TiDB sync finished with code ${code}!`);
    process.exit(code);
  });
}

main().catch(err => {
  console.error("❌ Fatal Error:", err);
  process.exit(1);
});

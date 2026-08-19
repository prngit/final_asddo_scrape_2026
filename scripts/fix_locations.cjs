require('dotenv').config();
const sqlite3 = require('sqlite3').verbose();
const mysql = require('mysql2/promise');
const path = require('path');

const SQLITE_DB = path.join(__dirname, '../master.db');

function normalizeName(str) {
  if (!str) return '';
  return str.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function capitalizeReason(r) {
  if (!r) return r;
  const lower = r.toLowerCase().trim();
  if (lower === 'absent') return 'Absent';
  if (lower === 'shifted') return 'Shifted';
  if (lower === 'death' || lower === 'dead') return 'Death';
  if (lower === 'duplicate') return 'Duplicate';
  if (lower === 'others' || lower === 'other') return 'Others';
  return r.charAt(0).toUpperCase() + r.slice(1);
}

async function main() {
  console.log("🔌 Connecting to TiDB to fetch canonical booth_metadata...");
  const tidb = await mysql.createConnection({
    uri: process.env.TIDB_URI,
    ssl: { rejectUnauthorized: true }
  });

  const [metadataRows] = await tidb.query("SELECT district, ac_no, part_no FROM booth_metadata");
  console.log(`✅ Loaded ${metadataRows.length} canonical metadata rows.`);
  await tidb.end();

  // Build lookup maps
  // 1. AC Map: normalized AC name -> canonical ac_no (and district)
  // 2. Exact Part Map: `${canonical_ac_no}___${part_num}` -> { district, ac_no, part_no }
  const acMap = new Map(); // e.g. "rajarajeshwarinagar" -> "154-Rajarajeshwarinagar"
  const partMap = new Map(); // e.g. "154-Rajarajeshwarinagar___125" -> { district, ac_no, part_no }
  const acToDistrict = new Map(); // e.g. "154-Rajarajeshwarinagar" -> "Bangalore Central"

  for (const row of metadataRows) {
    const canonicalDistrict = row.district;
    const canonicalAc = row.ac_no;
    const canonicalPart = row.part_no;

    acToDistrict.set(canonicalAc, canonicalDistrict);

    // Extract AC name without leading number, e.g. "154-Rajarajeshwarinagar" -> "Rajarajeshwarinagar"
    const acParts = canonicalAc.split('-');
    const acNum = acParts[0].trim();
    const acNameOnly = acParts.slice(1).join('-').trim();

    acMap.set(normalizeName(canonicalAc), canonicalAc);
    acMap.set(normalizeName(acNameOnly), canonicalAc);
    acMap.set(acNum, canonicalAc);

    // Extract part number from part_no, e.g. "125-School name" -> "125"
    const partNum = canonicalPart.split('-')[0].trim();
    partMap.set(`${canonicalAc}___${partNum}`, {
      district: canonicalDistrict,
      ac_no: canonicalAc,
      part_no: canonicalPart
    });
  }

  console.log(`🗺️ Built AC Map (${acMap.size} keys) and Part Map (${partMap.size} keys).`);

  // Open SQLite
  console.log("🔌 Connecting to SQLite master.db...");
  const db = new sqlite3.Database(SQLITE_DB);

  // We want to process all voters that were backfilled (have $.age or $.github_reason or voter_name not null)
  console.log("🔍 Fetching backfilled voters needing location standardization...");
  
  const query = `
    SELECT id, epic_no, district, ac_no, part_no, serial_no, voter_name, relative_details, uncollectable_reason, data
    FROM asddo_voters
    WHERE json_extract(data, '$.age') IS NOT NULL
  `;

  const rows = await new Promise((resolve, reject) => {
    db.all(query, [], (err, res) => {
      if (err) reject(err);
      else resolve(res);
    });
  });

  console.log(`📋 Found ${rows.length} backfilled rows to standardize.`);

  let matchedAcCount = 0;
  let matchedPartCount = 0;
  let unmatchedCount = 0;

  db.run("BEGIN TRANSACTION");
  const updateStmt = db.prepare(`
    UPDATE asddo_voters
    SET district = ?, ac_no = ?, part_no = ?, uncollectable_reason = ?, data = ?
    WHERE id = ?
  `);

  let processed = 0;
  for (const row of rows) {
    let payload;
    try {
      payload = typeof row.data === 'string' ? JSON.parse(row.data) : (row.data || {});
    } catch(e) {
      payload = {};
    }

    // Determine raw AC and raw Part
    const rawAc = row.ac_no || payload.acNo || '';
    const rawPart = (row.part_no !== null && row.part_no !== undefined ? String(row.part_no) : '') || 
                    (payload.partNo !== null && payload.partNo !== undefined ? String(payload.partNo) : '');

    // Resolve canonical AC
    let canonicalAc = acMap.get(normalizeName(rawAc)) || acMap.get(rawAc) || rawAc;
    if (canonicalAc !== rawAc) matchedAcCount++;

    // Resolve canonical Part & District
    let rawPartNum = rawPart.split('-')[0].trim();
    let canonicalDistrict = acToDistrict.get(canonicalAc) || row.district || payload.district || '';
    let canonicalPart = rawPart;

    const matchedPartInfo = partMap.get(`${canonicalAc}___${rawPartNum}`);
    if (matchedPartInfo) {
      canonicalDistrict = matchedPartInfo.district;
      canonicalAc = matchedPartInfo.ac_no;
      canonicalPart = matchedPartInfo.part_no;
      matchedPartCount++;
    } else {
      unmatchedCount++;
    }

    // Capitalize reason
    const reasonRaw = capitalizeReason(row.uncollectable_reason || payload.reasonRaw || payload.github_reason || '');

    // Build complete updated JSON payload
    payload.epic = row.epic_no;
    payload.district = canonicalDistrict;
    payload.acNo = canonicalAc;
    payload.partNo = canonicalPart;
    payload.slno = row.serial_no;
    payload.name = row.voter_name;
    payload.relativeName = row.relative_details;
    payload.reasonRaw = reasonRaw;
    if (payload.github_reason) {
      payload.github_reason = reasonRaw.toLowerCase();
    }

    const updatedDataJson = JSON.stringify(payload);

    updateStmt.run([
      canonicalDistrict,
      canonicalAc,
      canonicalPart,
      reasonRaw,
      updatedDataJson,
      row.id
    ]);

    processed++;
    if (processed % 50000 === 0 || processed === rows.length) {
      process.stdout.write(`\r⏳ Standardized ${processed} / ${rows.length} rows... (ACs matched: ${matchedAcCount}, Parts matched: ${matchedPartCount}, Parts fallback: ${unmatchedCount})`);
    }
  }

  console.log("\n💾 Finalizing SQLite transaction...");
  await new Promise((resolve, reject) => {
    updateStmt.finalize((err) => {
      if (err) reject(err);
      else resolve();
    });
  });

  await new Promise((resolve, reject) => {
    db.run("COMMIT", (err) => {
      if (err) reject(err);
      else resolve();
    });
  });

  console.log("✅ Local SQLite database successfully standardized!");
  db.close();
}

main().catch(err => {
  console.error("❌ Error in standardization:", err);
  process.exit(1);
});

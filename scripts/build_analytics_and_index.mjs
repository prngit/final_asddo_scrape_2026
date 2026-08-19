import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';

const ROOT = process.cwd();
const EXTRACTED_DIR = path.join(ROOT, 'cache', 'extracted');
const OUT_DIR = path.join(ROOT, 'data');
fs.mkdirSync(OUT_DIR, { recursive: true });

async function main() {
  console.log('🚀 Building Plaintext Analytics and Constituency Indexes...');
  
  if (!fs.existsSync(EXTRACTED_DIR)) {
    console.log('No cache/extracted directory found. Run extraction first.');
    return;
  }

  const files = fs.readdirSync(EXTRACTED_DIR).filter(f => f.endsWith('.ndjson'));
  console.log(`Found ${files.length} constituency NDJSON files.`);

  const stats = {
    totalRecords: 0,
    byCategory: { absent: 0, shifted: 0, death: 0, duplicate: 0, others: 0 },
    byDistrict: {},
    byConstituency: {},
    ageBands: { '18-29': 0, '30-44': 0, '45-59': 0, '60-79': 0, '80+': 0, unknown: 0 },
    topConstituencies: []
  };

  for (const file of files) {
    const filePath = path.join(EXTRACTED_DIR, file);
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath, 'utf8'),
      crlfDelay: Infinity
    });

    for await (const line of rl) {
      if (!line.trim()) continue;
      try {
        const row = JSON.parse(line);
        stats.totalRecords++;

        // Category tally
        const cat = (row.category || 'others').toLowerCase();
        if (stats.byCategory[cat] !== undefined) stats.byCategory[cat]++;
        else stats.byCategory.others++;

        // District tally
        const dist = row.district || 'Unknown';
        if (!stats.byDistrict[dist]) {
          stats.byDistrict[dist] = { total: 0, absent: 0, shifted: 0, death: 0, duplicate: 0, others: 0 };
        }
        stats.byDistrict[dist].total++;
        if (stats.byDistrict[dist][cat] !== undefined) stats.byDistrict[dist][cat]++;

        // Constituency tally
        const ac = row.acName || file.replace('.ndjson', '');
        if (!stats.byConstituency[ac]) {
          stats.byConstituency[ac] = { name: ac, district: dist, total: 0, absent: 0, shifted: 0, death: 0, duplicate: 0, others: 0 };
        }
        stats.byConstituency[ac].total++;
        if (stats.byConstituency[ac][cat] !== undefined) stats.byConstituency[ac][cat]++;

        // Age tally
        const age = row.age;
        if (!age) stats.ageBands.unknown++;
        else if (age < 30) stats.ageBands['18-29']++;
        else if (age < 45) stats.ageBands['30-44']++;
        else if (age < 60) stats.ageBands['45-59']++;
        else if (age < 80) stats.ageBands['60-79']++;
        else stats.ageBands['80+']++;
      } catch (e) {}
    }
  }

  // Top 25 constituencies
  stats.topConstituencies = Object.values(stats.byConstituency)
    .sort((a, b) => b.total - a.total)
    .slice(0, 25);

  const statsOut = path.join(OUT_DIR, 'analytics.json');
  fs.writeFileSync(statsOut, JSON.stringify(stats, null, 2), 'utf8');
  console.log(`✅ Analytics JSON written to: ${statsOut}`);
  console.log(`📊 Summary: ${stats.totalRecords.toLocaleString()} total voters across ${Object.keys(stats.byDistrict).length} districts.`);
}

main().catch(console.error);

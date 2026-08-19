/* Custom single-district discovery and extraction runner with custom Drive URL override.
 * Usage:
 *   node scraper_scripts/custom-extract.mjs --district "BAGALKOT" --url "https://drive.google.com/drive/folders/..."
 */

import { resolve } from 'node:path';
import {
  CACHE, ROOT, log, pool, progress, readJson, writeJson
} from './lib/common.mjs';
import { execSync, spawn } from 'node:child_process';
import fs from 'node:fs';

const args = process.argv.slice(2);
const argValue = (flag) => {
  const i = args.indexOf(flag);
  return i === -1 ? null : args[i + 1];
};

const district = argValue('--district')?.trim().toUpperCase();
const customUrl = argValue('--url')?.trim();

if (!district) {
  console.error("❌ Error: --district is required (e.g. --district BAGALKOT)");
  process.exit(1);
}

async function main() {
  log(`🎯 Targeted District Extraction requested for: ${district}`);
  if (customUrl) {
    log(`🔗 Custom Google Drive URL provided: ${customUrl}`);
    // Inject into seed/extra-sources.json so 1-discover uses this exact URL
    const extraPath = resolve(ROOT, 'seed', 'extra-sources.json');
    let extra = readJson(extraPath, { districts: {} }) || { districts: {} };
    if (!extra.districts) extra.districts = {};
    extra.districts[district] = [customUrl];
    writeJson(extraPath, extra);
    log(`✅ Saved custom source to seed/extra-sources.json`);
  }

  log(`\n1️⃣ Running 1-discover.mjs for ${district}...`);
  execSync(`node scraper_scripts/1-discover.mjs --district "${district}"`, { stdio: 'inherit' });

  log(`\n2️⃣ Running 2-extract.mjs for ${district}...`);
  execSync(`node scraper_scripts/2-extract.mjs --district "${district}"`, { stdio: 'inherit' });

  log(`\n🎉 Targeted extraction completed successfully for ${district}!`);
}

main().catch((err) => {
  console.error("❌ Error in custom extract:", err);
  process.exit(1);
});

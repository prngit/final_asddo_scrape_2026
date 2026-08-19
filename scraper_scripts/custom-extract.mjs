/* Multi-district targeted discovery and extraction runner.
 * Supports comma-separated district lists (e.g. "BAGALKOT, CHIKKABALLAPUR, KOLAR, KOPPAL")
 * and optional custom URLs (can be comma-separated or JSON map).
 */

import { resolve } from 'node:path';
import {
  CACHE, ROOT, log, readJson, writeJson
} from './lib/common.mjs';
import { execSync } from 'node:child_process';

const args = process.argv.slice(2);
const argValue = (flag) => {
  const i = args.indexOf(flag);
  return i === -1 ? null : args[i + 1];
};

const rawDistricts = argValue('--district');
const customUrl = argValue('--url')?.trim();

if (!rawDistricts) {
  console.error("❌ Error: --district is required (e.g. --district 'BAGALKOT, CHIKKABALLAPUR, KOLAR, KOPPAL')");
  process.exit(1);
}

const districts = rawDistricts
  .split(',')
  .map(d => d.trim().toUpperCase())
  .filter(Boolean);

async function main() {
  log(`🎯 Targeted Multi-District Extraction requested for (${districts.length} districts): ${districts.join(', ')}`);

  if (customUrl) {
    log(`🔗 Custom URL provided: ${customUrl}`);
    const extraPath = resolve(ROOT, 'seed', 'extra-sources.json');
    let extra = readJson(extraPath, { districts: {} }) || { districts: {} };
    if (!extra.districts) extra.districts = {};

    // If single custom URL provided and multiple districts, assign or parse
    for (const dist of districts) {
      extra.districts[dist] = [customUrl];
    }
    writeJson(extraPath, extra);
    log(`✅ Saved custom source(s) to seed/extra-sources.json`);
  }

  for (const dist of districts) {
    log(`\n======================================================`);
    log(`🚀 Processing District: ${dist}`);
    log(`======================================================`);

    try {
      log(`1️⃣ Running 1-discover.mjs for ${dist}...`);
      execSync(`node scraper_scripts/1-discover.mjs --district "${dist}"`, { stdio: 'inherit' });

      log(`2️⃣ Running 2-extract.mjs for ${dist} (--force)...`);
      execSync(`node scraper_scripts/2-extract.mjs --district "${dist}" --force`, { stdio: 'inherit' });

      log(`✅ Successfully finished extraction for ${dist}!`);
    } catch (err) {
      console.error(`⚠️ Extraction encountered an issue for ${dist}:`, err.message);
    }
  }

  log(`\n🎉 Multi-district run finished for: ${districts.join(', ')}`);
}

main().catch((err) => {
  console.error("❌ Fatal Error in multi-extract:", err);
  process.exit(1);
});

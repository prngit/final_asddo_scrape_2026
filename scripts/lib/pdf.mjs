import { inflateSync, unzipSync } from 'node:zlib';

/**
 * Categorizes raw reason text into official canonical categories.
 */
export function categorise(reasonRaw) {
  const r = (reasonRaw || '').toLowerCase();
  if (!r) return 'others';
  if (/already enrolled|duplicate|repeat/.test(r)) return 'duplicate';
  if (/death|dead|expired|deceased/.test(r)) return 'death';
  if (/shift|migrat|moved|permanent/.test(r)) return 'shifted';
  if (/absent|untrace|not found|non-?resident/.test(r)) return 'absent';
  return 'others';
}

export const CATEGORIES = ['absent', 'shifted', 'death', 'duplicate', 'others'];

/**
 * Extracts and decodes PDF text blocks accurately from binary streams.
 */
export function extractItems(buf) {
  const streams = [];
  const START = Buffer.from('stream');
  const END = Buffer.from('endstream');
  let from = 0;
  for (;;) {
    const s = buf.indexOf(START, from);
    if (s === -1) break;
    const e = buf.indexOf(END, s);
    if (e === -1) break;
    let a = s + START.length;
    if (buf[a] === 0x0d) a++;
    if (buf[a] === 0x0a) a++;
    let b = e;
    if (buf[b - 1] === 0x0a) b--;
    if (buf[b - 1] === 0x0d) b--;
    streams.push(buf.subarray(a, b));
    from = e + END.length;
  }

  const pages = [];
  for (const raw of streams) {
    let data = raw;
    if ((raw[0] & 0x0f) === 8) {
      try {
        data = inflateSync(raw);
      } catch {
        try {
          data = unzipSync(raw);
        } catch {
          continue;
        }
      }
    }
    const text = data.toString('latin1');
    const items = [];
    const re = /(?:\[([^\]]+)\]\s*TJ|\(([^)]*)\)\s*Tj)/g;
    let match;
    while ((match = re.exec(text)) !== null) {
      const str = match[2] !== undefined ? match[2] : match[1];
      if (str) items.push(str);
    }
    if (items.length) pages.push(items);
  }
  return pages;
}

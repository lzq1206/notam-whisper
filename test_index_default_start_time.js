#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  if (start === -1) return null;
  const braceStart = src.indexOf('{', start);
  if (braceStart === -1) return null;
  let depth = 0;
  for (let i = braceStart; i < src.length; i++) {
    const ch = src[i];
    if (ch === '{') depth++;
    if (ch === '}') {
      depth--;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return null;
}

const setDefaultDatesBlock = extractFunctionBlock(html, 'function setDefaultDates()');
if (!setDefaultDatesBlock) {
  throw new Error('setDefaultDates function not found');
}

if (/localStorage\.getItem\(['"]notam_start_date['"]\)/.test(setDefaultDatesBlock)) {
  throw new Error('setDefaultDates should not read notam_start_date from localStorage');
}

if (!/const\s+start\s*=\s*today\s*;/.test(setDefaultDatesBlock)) {
  throw new Error('setDefaultDates should default start to today');
}

if (/localStorage\.setItem\(['"]notam_start_date['"]/.test(html)) {
  throw new Error('index.html should not persist notam_start_date');
}

console.log('test_index_default_start_time.js passed');

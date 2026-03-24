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

const notamLocalFn = extractFunctionBlock(html, 'function formatUtcWithLocal(');
if (!notamLocalFn) {
  throw new Error('formatUtcWithLocal function not found');
}

if (!/Date\.parse\(utc\)/.test(notamLocalFn) || !/toLocaleString\(\)/.test(notamLocalFn)) {
  throw new Error('formatUtcWithLocal should parse UTC and include local time');
}

const launchLocalFn = extractFunctionBlock(html, 'function formatLaunchDateWithLocal(');
if (!launchLocalFn) {
  throw new Error('formatLaunchDateWithLocal function not found');
}

if (!/toISOString\(\)/.test(launchLocalFn) || !/replace\('T', ' '\)/.test(launchLocalFn) || !/slice\(0,\s*16\)/.test(launchLocalFn)) {
  throw new Error('formatLaunchDateWithLocal should build UTC text from ISO timestamp');
}
if (!/toLocaleString\(\)/.test(launchLocalFn) || !/Local/.test(launchLocalFn)) {
  throw new Error('formatLaunchDateWithLocal should include local time');
}

if (!/from:\s*\$\{formatUtcWithLocal\(r\.from_utc\)\}/.test(html)) {
  throw new Error('NOTAM/MSI from time should use formatUtcWithLocal');
}
if (!/to:\s*\$\{formatUtcWithLocal\(r\.to_utc\)\}/.test(html)) {
  throw new Error('NOTAM/MSI to time should use formatUtcWithLocal');
}

if (!/const dateStr = formatLaunchDateWithLocal\(t0\);/.test(html)) {
  throw new Error('Upcoming launch time should use formatLaunchDateWithLocal');
}
if (!/const dateStr = formatLaunchDateWithLocal\(d\);/.test(html)) {
  throw new Error('Past launch time should use formatLaunchDateWithLocal');
}

console.log('test_index_local_time_display.js passed');

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
    if (src[i] === '{') depth++;
    if (src[i] === '}') {
      depth--;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return null;
}

const parseDescriptionDateBlock = extractFunctionBlock(html, 'function parseDescriptionDate(');
const parseLaunchTimestampValueBlock = extractFunctionBlock(html, 'function parseLaunchTimestampValue(');
const launchTimestampBlock = extractFunctionBlock(html, 'function launchTimestamp(');
const filterFutureLaunchesBlock = extractFunctionBlock(html, 'function filterFutureLaunches(');
if (!parseDescriptionDateBlock || !parseLaunchTimestampValueBlock || !launchTimestampBlock || !filterFutureLaunchesBlock) {
  throw new Error('launch expiry helpers are missing');
}
global.eval(parseDescriptionDateBlock);
global.eval(parseLaunchTimestampValueBlock);
global.eval(launchTimestampBlock);
global.eval(filterFutureLaunchesBlock);

const now = Date.parse('2026-09-16T01:30:00Z');
const records = [
  { name: 'Sentinel-3C & FLEX', win_open: '2026-09-15T01:21:00Z' },
  { name: 'Shanghai Sea Launch', win_open: '2026-09-15T21:55:00Z' },
  { name: 'Progress MS-35 (96P)', win_open: '2026-09-16T13:33:00Z' },
  { name: 'Time TBD', win_open: '' },
];
const active = filterFutureLaunches(records, now);
const activeNames = active.map(record => record.name);

if (activeNames.includes('Sentinel-3C & FLEX') || activeNames.includes('Shanghai Sea Launch')) {
  throw new Error('elapsed manually scheduled launches must be hidden from the upcoming layer');
}
if (!activeNames.includes('Progress MS-35 (96P)') || !activeNames.includes('Time TBD')) {
  throw new Error('future and time-undisclosed launches must remain visible');
}

const exactBoundary = filterFutureLaunches([
  { name: 'At launch time', win_open: '2026-09-16T01:30:00Z' },
], now);
if (exactBoundary.length !== 0) {
  throw new Error('a launch must stop being upcoming at its scheduled UTC time');
}

if (!/const\s+nowMs\s*=\s*now\.getTime\(\);[\s\S]*?filterFutureLaunches\(localUpcoming,\s*nowMs\)[\s\S]*?filterFutureLaunches\(remoteUpcoming,\s*nowMs\)/.test(html)) {
  throw new Error('local and remote launch records must share the future-time filter before merging');
}
if (!/scheduleUpcomingLaunchExpiry\(\);/.test(html) || !/launchRecord\.marker\s*=\s*lMarker;/.test(html)) {
  throw new Error('upcoming markers must be scheduled for automatic expiry');
}

console.log('test_index_launch_expiry.js passed');

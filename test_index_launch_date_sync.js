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
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return null;
}

const rangeBlock = extractFunctionBlock(html, 'function getNotamLaunchDateRange(');
const predicateBlock = extractFunctionBlock(html, 'function isUpcomingLaunchInNotamRange(');
const listBlock = extractFunctionBlock(html, 'function upcomingLaunchesForNotamRange(');
if (!rangeBlock || !predicateBlock || !listBlock) {
  throw new Error('NOTAM-synchronized upcoming-launch helpers are missing');
}
global.eval(rangeBlock);
global.eval(predicateBlock);
global.eval(listBlock);

const range = getNotamLaunchDateRange('2026-09-22', '2026-09-29');
const launches = [
  { name: 'at-start', ts: Date.parse('2026-09-22T00:00:00Z') },
  { name: 'inside', ts: Date.parse('2026-09-25T12:00:00Z') },
  { name: 'at-end', ts: Date.parse('2026-09-29T23:59:59Z') },
  { name: 'before', ts: Date.parse('2026-09-21T23:59:59Z') },
  { name: 'after', ts: Date.parse('2026-09-30T00:00:00Z') },
  { name: 'time-tbd', ts: null },
];
const visible = upcomingLaunchesForNotamRange(launches, range).map(launch => launch.name);
const expected = ['at-start', 'inside', 'at-end'];
if (JSON.stringify(visible) !== JSON.stringify(expected)) {
  throw new Error(`launch date-range filtering is incorrect: ${JSON.stringify(visible)}`);
}

const resetVisible = upcomingLaunchesForNotamRange(launches, getNotamLaunchDateRange('', ''))
  .map(launch => launch.name);
if (!resetVisible.includes('time-tbd') || resetVisible.length !== launches.length) {
  throw new Error('blank NOTAM date controls should restore all unexpired upcoming launches');
}

if (!/syncUpcomingLaunchLayerToNotamRange\(\);/.test(html)) {
  throw new Error('rendering must synchronize the 2D upcoming-launch layer with the NOTAM range');
}
if (!/upcomingLaunchesForNotamRange\(\)\.forEach\(launch =>/.test(html)) {
  throw new Error('Cesium upcoming launches must use the NOTAM date-range subset');
}
if (!/const\s+launchRows\s*=\s*launchToggle\.checked\s*\?\s*upcomingLaunchesForNotamRange\(\)\s*:\s*\[\]/.test(html)) {
  throw new Error('current-view KML must use the NOTAM date-range subset');
}
if (!/function\s+applyFilter\([\s\S]*?renderRows\(filtered\)/.test(html) ||
  !/function\s+renderRows\(rows\)\s*\{[\s\S]*?syncUpcomingLaunchLayerToNotamRange\(\);/.test(html)) {
  throw new Error('applying the NOTAM filter must rebuild the synchronized launch layer');
}

console.log('test_index_launch_date_sync.js passed');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const root = __dirname;
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const notams = fs.readFileSync(path.join(root, 'notams.csv'), 'utf8').replace(/^\uFEFF/, '');

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

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  const headers = lines.shift().split(',');
  return lines.map(line => {
    const values = [];
    let value = '';
    let quoted = false;
    for (let i = 0; i < line.length; i += 1) {
      const char = line[i];
      if (char === '"') {
        if (quoted && line[i + 1] === '"') { value += '"'; i += 1; }
        else quoted = !quoted;
      } else if (char === ',' && !quoted) {
        values.push(value); value = '';
      } else value += char;
    }
    values.push(value);
    return Object.fromEntries(headers.map((header, index) => [header, values[index] || '']));
  });
}

const signatures = [
  'function launchDistanceKm(',
  'function launchProviderName(',
  'function warningTimeMatchesLaunch(',
  'function warningScheduleAllowsLaunch(',
  'function warningRectanglePointsToLaunch(',
  'function launchCorridorPairMatches(',
  'function relatedWarningRowsForLaunch(',
  'function launchWarningArcBetween(',
  'function selectedLaunchWarningSegments(',
];
const blocks = signatures.map(signature => {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} not found`);
  return block;
});

global.isMsiOrMaritime = row => row && row.country === 'Maritime';
global.selectedLaunchDirection = null;
global.launchToggle = { checked: true };
global.filteredDataRows = [];
global.upcomingLaunchesForNotamRange = () => [global.selectedLaunchDirection];
for (const block of blocks) global.eval(block);

const rows = parseCsv(notams).filter(row => ['B5163/26', 'B5164/26'].includes(row.notam_id));
if (rows.length !== 2) throw new Error(`Expected B5163/26 and B5164/26 in notams.csv, got ${rows.length}`);

// The published launch snapshot is one UTC day stale (22 Oct 07:00Z), while
// the explicit Rocket Lab debris-return warnings are valid on 23 Oct.
const launch = {
  mission: "Electron | We're Not Hawking Around (3x HawkEye 360)",
  providerName: 'Rocket Lab',
  ts: Date.parse('2026-10-22T07:00:00Z'),
  lat: -39.262833,
  lon: 177.864469,
};
global.selectedLaunchDirection = launch;
global.filteredDataRows = rows;

const related = relatedWarningRowsForLaunch(launch, rows);
if (related.length !== 2 || related.map(row => row.notam_id).sort().join(',') !== 'B5163/26,B5164/26') {
  throw new Error(`Explicit Rocket Lab debris chain did not match both warnings: ${related.map(row => row.notam_id).join(',')}`);
}

const segments = selectedLaunchWarningSegments();
if (segments.length !== 2) throw new Error(`Expected two sequential direction segments, got ${segments.length}`);
if (segments[0].from !== launch || segments[0].to.notam_id !== 'B5163/26') {
  throw new Error('Nearest warning must be the first segment target from the launch pad');
}
if (segments[1].from !== rows.find(row => row.notam_id === 'B5163/26') || segments[1].to.notam_id !== 'B5164/26') {
  throw new Error('Farther warning must be targeted from the previous warning, not from the launch pad');
}
if (segments.some(segment => segment.points.length < 2)) throw new Error('Sequential great-circle segments must contain sampled points');

const wrongProvider = { ...launch, providerName: 'SpaceX' };
if (relatedWarningRowsForLaunch(wrongProvider, rows).length !== 0) {
  throw new Error('Explicit Rocket Lab activity must not match a different launch provider');
}

console.log('test_index_launch_notam_direction.js passed');

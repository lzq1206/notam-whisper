#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const root = __dirname;
const sites = fs.readFileSync(path.join(root, 'launch_sites.csv'), 'utf8');
const upcoming = fs.readFileSync(path.join(root, 'upcoming_launches.csv'), 'utf8');
const notams = fs.readFileSync(path.join(root, 'notams.csv'), 'utf8');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');

if (!/\n51,Shanghai Sea Launch,SHSL,China,31\.200000,123\.700000,Orbital \(Sea\),Active\n/.test(sites)) {
  throw new Error('Shanghai Sea Launch site must be present at the A4352/26 center');
}
if (!/2026 SEP 15 2155,SHSL,31\.2,123\.7,TBD,Shanghai Sea Launch,Shanghai Sea Launch/.test(upcoming)) {
  throw new Error('Shanghai Sea Launch local schedule must use 2026-09-15 21:55 UTC');
}
if (!/A4352\/26,ZSHA,2026-09-15T21:50:00Z,2026-09-15T22:11:00Z,31\.2,123\.7,011,QRDCA/.test(notams)) {
  throw new Error('A4352/26 must remain centered at 31.2,123.7 with the supplied time window');
}
if (!/LOCAL_UPCOMING_LAUNCHES_URL/.test(html) || !/sourceLabel: 'Data by local schedule'/.test(html)) {
  throw new Error('index.html must load and label the local upcoming launch record');
}

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  const braceStart = src.indexOf('{', start);
  let depth = 0;
  for (let i = braceStart; i < src.length; i++) {
    if (src[i] === '{') depth++;
    if (src[i] === '}' && --depth === 0) return src.slice(start, i + 1);
  }
  return null;
}

for (const signature of [
  'function launchDistanceKm(',
  'function warningTimeMatchesLaunch(',
  'function warningScheduleAllowsLaunch(',
  'function launchCorridorPairMatches(',
  'function relatedWarningRowsForLaunch(',
]) {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} must remain available for launch matching`);
  global.eval(block);
}

const launch = {
  mission: 'Shanghai Sea Launch',
  ts: Date.parse('2026-09-15T21:55:00Z'),
  lat: 31.2,
  lon: 123.7,
};
const warning = {
  notam_id: 'A4352/26',
  from_utc: '2026-09-15T21:50:00Z',
  to_utc: '2026-09-15T22:11:00Z',
  lat: '31.2',
  lon: '123.7',
  raw: 'A4352/26 NOTAMN Q)ZSHA/QRDCA/IV/BO/W/000/999/3112N12342E011',
};
if (relatedWarningRowsForLaunch(launch, [warning]).length !== 1) {
  throw new Error('2026-09-15 21:55 UTC launch must match A4352/26 at the Shanghai Sea Launch site');
}

console.log('test_shanghai_sea_launch.js passed');

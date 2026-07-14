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

const signatures = [
  'function escapeHtml(',
  'function notamGeometryKey(',
  'function overlappingNotamRows(',
  'function launchDistanceKm(',
  'function relatedLaunchesForRows(',
  'function relatedWarningRowsForLaunch(',
  'function notamDisplayTitle(',
  'function buildNotamPopup(',
];
const blocks = signatures.map(signature => {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} not found`);
  return block;
});

global.isMsiOrMaritime = row => row && row.country === 'Maritime';
global.formatUtcWithLocal = value => `${value} / Local`;
global.upcomingLaunches = [];
global.t = (key, vars = {}) => {
  if (key === 'overlapping_notams') return `Overlapping NOTAMs (${vars.count})`;
  if (key === 'related_launch') return 'Related launch';
  return key;
};
for (const block of blocks) global.eval(block);

const polygon = JSON.stringify([[28.1, -80.1], [28.2, -80.2], [28.3, -80.1]]);
const ids = ['FDC 6/5192', 'FDC 6/5190', 'FDC 6/5189', 'FDC 6/5187', 'FDC 6/5186', 'FDC 6/5185'];
const rows = ids.map((notam_id, index) => ({
  country: 'USA',
  notam_id,
  polygon,
  fir: index % 2 ? 'ZMA' : 'ZJX',
  from_utc: `2026-07-${14 + Math.floor(index / 2)}T07:05:00Z`,
  to_utc: `2026-07-${14 + Math.floor(index / 2)}T11:57:00Z`,
  qcode: 'TFR91.143',
  raw: `${notam_id} <SPACE OPS>`,
}));

const grouped = overlappingNotamRows(rows[0], rows);
if (grouped.length !== 6) throw new Error(`Expected 6 overlapping NOTAMs, got ${grouped.length}`);
const popup = buildNotamPopup(grouped[0], rows);
for (const id of ids) {
  if (!popup.includes(id)) throw new Error(`Popup missing ${id}`);
}
if (!popup.includes('Overlapping NOTAMs (6)')) throw new Error('Popup missing overlap count');
if (popup.includes('<SPACE OPS>')) throw new Error('Raw HTML was not escaped');

global.upcomingLaunches = [{
  mission: 'Starship Flight 13',
  ts: Date.parse('2026-07-16T22:45:00Z'),
  lat: 25.997,
  lon: -97.156,
}];
const starbasePolygon = JSON.stringify([[25.90, -97.20], [26.10, -97.20], [26.10, -97.00]]);
const starbaseRows = [
  {
    country: 'USA', notam_id: 'FDC 6/5373', polygon: starbasePolygon,
    fir: 'ZHU', from_utc: '2026-07-16T22:30:00Z', to_utc: '2026-07-17T00:54:00Z',
    lat: '26.011905', lon: '-97.057143', raw: 'SPACE OPERATIONS',
  },
  {
    country: 'USA', notam_id: 'FDC 6/5388', polygon: starbasePolygon,
    fir: 'ZHU', from_utc: '2026-07-17T22:30:00Z', to_utc: '2026-07-18T00:54:00Z',
    lat: '26.011905', lon: '-97.057143', raw: 'SPACE OPERATIONS',
  },
];
const starbasePopup = buildNotamPopup(starbaseRows[0], starbaseRows);
if (!starbasePopup.includes('Starship Flight 13')) throw new Error('Starship mission must be linked to its active TFR');
if (!starbasePopup.includes('Related launch')) throw new Error('Related launch label is missing');

const mismatchedVandenbergWarning = [{
  country: 'Maritime', notam_id: 'NAVAREA XII 472/26',
  from_utc: '2026-07-16T02:00:00Z', to_utc: '2026-07-16T06:43:00Z',
  lat: '34.4', lon: '-120.35', raw: 'ROCKET LAUNCHING',
}];
const trancheLaunch = [{
  mission: 'Tranche 1 Transport Layer E', ts: Date.parse('2026-07-16T20:32:00Z'),
  lat: 34.4, lon: -120.35,
}];
if (relatedLaunchesForRows(mismatchedVandenbergWarning, trancheLaunch).length !== 0) {
  throw new Error('Time-mismatched Vandenberg warning must not be attributed to Tranche');
}
if (relatedWarningRowsForLaunch(trancheLaunch[0], mismatchedVandenbergWarning).length !== 0) {
  throw new Error('Tranche launch popup must not list the time-mismatched warning');
}

const renderRowsBlock = extractFunctionBlock(html, 'function renderRows(');
const renderGlobeBlock = extractFunctionBlock(html, 'function renderOnGlobe(');
for (const [name, block] of [['renderRows', renderRowsBlock], ['renderOnGlobe', renderGlobeBlock]]) {
  if (!block || !/overlapping\[0\]\s*!==\s*r/.test(block)) {
    throw new Error(`${name} must suppress duplicate overlapping geometry`);
  }
  if (!/buildNotamPopup\(r,\s*rows\)/.test(block)) {
    throw new Error(`${name} must use the aggregated popup`);
  }
}

console.log('test_index_overlapping_notams.js passed');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const root = __dirname;
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const notamsText = fs.readFileSync(path.join(root, 'notams.csv'), 'utf8').replace(/^\uFEFF/, '');
const launches = JSON.parse(fs.readFileSync(path.join(root, 'upcoming_launches.json'), 'utf8')).results || [];

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
  'function parseCoordinate(',
  'function parseNotamPolygons(',
  'function extractRowCoordinatesList(',
  'function launchDistanceKm(',
  'function launchProviderName(',
  'function warningTimeMatchesLaunch(',
  'function launchWarningIsNearPad(',
  'function warningWindowMatchesLaunchSchedule(',
  'function warningNearLaunchPadMatches(',
  'function warningScheduleAllowsLaunch(',
  'function warningRectanglePointsToLaunch(',
  'function launchCorridorPairMatches(',
  'function warningRegionCenters(',
  'function warningExplicitLaunchFamilyMatches(',
  'function warningMultiRegionCorridorMatches(',
  'function launchWarningDirectionTargets(',
  'function relatedWarningRowsForLaunch(',
  'function launchWarningArcBetween(',
  'function selectedLaunchWarningSegments(',
];
const blocks = signatures.map(signature => {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} not found`);
  return block;
});
const launchWarningConstant = html.match(/const LOCAL_LAUNCH_WARNING_DISTANCE_KM\s*=\s*120;/)?.[0];
if (!launchWarningConstant) throw new Error('LOCAL_LAUNCH_WARNING_DISTANCE_KM constant not found');

global.isMsiOrMaritime = row => row && (row.country === 'Maritime' || row.source === 'MSI' || row.type === 'MSI');
global.launchToggle = { checked: true };
global.selectedLaunchDirection = null;
global.filteredDataRows = [];
global.upcomingLaunchesForNotamRange = () => [global.selectedLaunchDirection];
global.eval([launchWarningConstant, ...blocks].join('\n'));

const row = parseCsv(notamsText).find(item => item.notam_id === 'A3241/26');
if (!row) throw new Error('A3241/26 is missing from the local NOTAM snapshot');
const source = launches.find(item => item.name === 'GSLV Mk II | IRNSS-1L (NVS-03)');
if (!source) throw new Error('GSLV Mk II | IRNSS-1L (NVS-03) is missing from the local launch snapshot');
const launch = {
  mission: source.name,
  vehicle: source.rocket?.configuration?.full_name || 'GSLV Mk. II',
  providerName: source.launch_service_provider?.name || 'Indian Space Research Organization',
  ts: Date.parse(source.net),
  windowStartTs: Date.parse(source.window_start),
  windowEndTs: Date.parse(source.window_end),
  lat: source.pad.latitude,
  lon: source.pad.longitude,
};

const regions = warningRegionCenters(row);
if (regions.length !== 2) throw new Error(`A3241/26 should expose two polygon regions, got ${regions.length}`);
if (!warningExplicitLaunchFamilyMatches(row, launch)) {
  throw new Error('A3241/26 should have explicit GSLV rocket-launch evidence');
}
if (!warningMultiRegionCorridorMatches(row, launch)) {
  throw new Error('A3241/26 two-region corridor should match the GSLV launch pad');
}
const related = relatedWarningRowsForLaunch(launch, [row]);
if (related.length !== 1 || related[0] !== row) {
  throw new Error(`A3241/26 should be associated once at row level, got ${related.length}`);
}
const targets = launchWarningDirectionTargets(row, launch);
if (targets.length !== 2 || targets.some((target, index) => target.regionIndex !== index)) {
  throw new Error('A3241/26 direction targets should retain two ordered region centers');
}

global.selectedLaunchDirection = launch;
global.filteredDataRows = [row];
const segments = selectedLaunchWarningSegments();
if (segments.length !== 2 || segments[0].row.regionIndex !== 0 || segments[1].row.regionIndex !== 1) {
  throw new Error('A3241/26 should produce pad-to-region-1-to-region-2 direction segments');
}
if (segments.some(segment => segment.points.length < 2)) {
  throw new Error('A3241/26 multi-region direction segments must be sampled great-circle paths');
}

const wrongFamily = { ...launch, mission: 'PSLV-XL | Unrelated Payload', vehicle: 'PSLV-XL' };
if (relatedWarningRowsForLaunch(wrongFamily, [row]).length !== 0) {
  throw new Error('A3241/26 must not match an unrelated launch family at the same time/site');
}
const wrongSite = { ...launch, lat: 20, lon: 100 };
if (relatedWarningRowsForLaunch(wrongSite, [row]).length !== 0) {
  throw new Error('A3241/26 must not match a remote GSLV launch site');
}
const maritimeRow = { ...row, country: 'Maritime', source: 'MSI' };
if (warningMultiRegionCorridorMatches(maritimeRow, launch)) {
  throw new Error('Maritime multi-region warnings must remain separate from aviation NOTAM matching');
}

console.log('test_index_multi_region_launch_matching.js passed');

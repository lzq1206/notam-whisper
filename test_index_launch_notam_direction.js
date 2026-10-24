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
  'function relatedWarningRowsForLaunch(',
  'function warningRowsMatch(',
  'function relatedLaunchesForWarningRows(',
  'function launchWarningArcBetween(',
  'function selectedLaunchWarningSegments(',
  'function buildCesiumUpcomingLaunchIcon(',
];
const blocks = signatures.map(signature => {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} not found`);
  return block;
});
const launchWarningConstant = html.match(/const LOCAL_LAUNCH_WARNING_DISTANCE_KM\s*=\s*120;/)?.[0];
if (!launchWarningConstant) throw new Error('LOCAL_LAUNCH_WARNING_DISTANCE_KM constant not found');

global.isMsiOrMaritime = row => row && row.country === 'Maritime';
global.btoa = value => Buffer.from(value, 'binary').toString('base64');
global.selectedLaunchDirection = null;
global.launchToggle = { checked: true };
global.filteredDataRows = [];
global.upcomingLaunchesForNotamRange = () => [global.selectedLaunchDirection];
global.eval([launchWarningConstant, ...blocks].join('\n'));

const rows = parseCsv(notams).filter(row => ['B5162/26', 'B5163/26', 'B5164/26'].includes(row.notam_id));
if (rows.length !== 3) throw new Error(`Expected B5162/26, B5163/26 and B5164/26 in notams.csv, got ${rows.length}`);

// The published launch snapshot is one UTC day stale (22 Oct 07:00Z), while
// the explicit Rocket Lab debris-return warnings are valid on 23 Oct.
const launch = {
  mission: "Electron | We're Not Hawking Around (3x HawkEye 360)",
  providerName: 'Rocket Lab',
  ts: Date.parse('2026-10-22T07:00:00Z'),
  windowStartTs: Date.parse('2026-10-22T07:00:00Z'),
  windowEndTs: Date.parse('2026-10-22T07:00:00Z'),
  lat: -39.262833,
  lon: 177.864469,
};
global.selectedLaunchDirection = launch;
global.filteredDataRows = rows;

const related = relatedWarningRowsForLaunch(launch, rows);
if (related.length !== 3 || related.map(row => row.notam_id).sort().join(',') !== 'B5162/26,B5163/26,B5164/26') {
  throw new Error(`Rocket Lab launch should match the local launch warning plus debris chain: ${related.map(row => row.notam_id).join(',')}`);
}
const b5163RelatedLaunches = relatedLaunchesForWarningRows(
  [rows.find(row => row.notam_id === 'B5163/26')], rows, [launch]
);
if (b5163RelatedLaunches.length !== 1 || b5163RelatedLaunches[0] !== launch) {
  throw new Error('A direction-eligible NOTAM must be able to identify its related launch from the full warning set');
}
if (!warningNearLaunchPadMatches(rows.find(row => row.notam_id === 'B5162/26'), launch)) {
  throw new Error('B5162/26 should match as an explicit near-pad Rocket Lab launch warning');
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
if (segments.some(segment => segment.row.notam_id === 'B5162/26')) {
  throw new Error('Near-pad B5162/26 must be displayed as related but excluded from direction segments');
}

const glowSvg = Buffer.from(buildCesiumUpcomingLaunchIcon({ isLaunchGlow: true }, '#ff0000').split(',')[1], 'base64').toString('utf8');
const plainSvg = Buffer.from(buildCesiumUpcomingLaunchIcon({ isLaunchGlow: false }, '#ff0000').split(',')[1], 'base64').toString('utf8');
if (!glowSvg.includes('feGaussianBlur') || !glowSvg.includes('stroke="#fff"')) {
  throw new Error('3D upcoming-launch glow icon must contain a visible white halo');
}
if (plainSvg.includes('feGaussianBlur')) throw new Error('Non-glow 3D launch icon must not contain the halo filter');

if (!/function selectNotamForSolarOverlay\([\s\S]*?relatedLaunchesForWarningRows\(\[row\], filteredDataRows\)/.test(html)) {
  throw new Error('2D NOTAM selection must resolve its related launch for direction rendering');
}
if (!/selectedLaunchDirection = entity\.__launchDirectionRecord \|\| selectedNotamSolarState\.launchDirection \|\| null/.test(html)) {
  throw new Error('3D NOTAM selection must carry its related launch direction record');
}
if (!/function buildNotamPopup\([\s\S]*?cardRelated = relatedLaunchesForWarningRows\(\[r\], rows\)/.test(html)) {
  throw new Error('NOTAM detail cards must show their related launch');
}
if (!/launchDirection: directionLaunches\[0\] \|\| null/.test(html)) {
  throw new Error('3D NOTAM entities must carry the related launch direction record');
}

const wrongProvider = { ...launch, providerName: 'SpaceX' };
if (relatedWarningRowsForLaunch(wrongProvider, rows).length !== 0) {
  throw new Error('Explicit Rocket Lab activity must not match a different launch provider');
}
const remoteRocketLab = { ...launch, lat: -35, lon: 150 };
if (relatedWarningRowsForLaunch(remoteRocketLab, rows).length !== 0) {
  throw new Error('A local Rocket Lab warning must not match a remote launch site');
}

console.log('test_index_launch_notam_direction.js passed');

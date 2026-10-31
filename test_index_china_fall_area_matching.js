#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const root = __dirname;
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');

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
];
const blocks = signatures.map(signature => {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} not found`);
  return block;
});
const launchWarningConstant = html.match(/const LOCAL_LAUNCH_WARNING_DISTANCE_KM\s*=\s*120;/)?.[0];
if (!launchWarningConstant) throw new Error('LOCAL_LAUNCH_WARNING_DISTANCE_KM constant not found');

global.isMsiOrMaritime = row => row && (row.country === 'Maritime' || row.source === 'MSI' || row.type === 'MSI');
global.eval([launchWarningConstant, ...blocks].join('\n'));

const b5826 = {
  country: 'PHILIPPINES',
  notam_id: 'B5826/26',
  qcode: 'QWMLW',
  from_utc: '2026-11-05T21:10:00Z',
  to_utc: '2026-11-05T21:39:00Z',
  lat: '9.108334',
  lon: '119.275',
  polygon: JSON.stringify([
    [9.266667, 118.95], [9.466667, 119.216667],
    [8.95, 119.6], [8.75, 119.333333],
  ]),
  raw: 'B5826/26 NOTAMN Q) RPHI/QWMLW/IV/BO/W/000/999/0907N11917E023 A) RPHI B) 2611052110 C) 2611052139 E) SPECIAL OPS (AEROSPACE FLT ACT) WILL BE CONDUCTED BY CHINA. EST FALL AREA OF UNBURNED DEBRIS WI: 091600N 1185700E - 092800N 1191300E - 085700N 1193600E - 084500N 1192000E - 091600N 1185700E.',
};
const longMarch12 = {
  mission: 'Long March 12 | SatNet LEO Group TBD?',
  vehicle: 'Long March 12',
  providerName: 'China Aerospace Science and Technology Corporation',
  ts: Date.parse('2026-11-05T21:15:00Z'),
  windowStartTs: Date.parse('2026-11-05T21:10:00Z'),
  windowEndTs: Date.parse('2026-11-05T21:39:00Z'),
  lat: 19.59755,
  lon: 110.936481,
};

if (!warningRectanglePointsToLaunch(b5826, longMarch12)) {
  throw new Error('B5826/26 should pass the narrow Chinese Long March fall-area geometry gate');
}
const related = relatedWarningRowsForLaunch(longMarch12, [b5826]);
if (related.length !== 1 || related[0] !== b5826) {
  throw new Error(`B5826/26 should be associated once at row level, got ${related.length}`);
}
const directionTargets = launchWarningDirectionTargets(b5826, longMarch12);
if (directionTargets.length !== 1 || directionTargets[0] !== b5826) {
  throw new Error('B5826/26 should be eligible as one validated downrange direction target');
}

const wrongProvider = { ...longMarch12, providerName: 'SpaceX' };
if (warningRectanglePointsToLaunch(b5826, wrongProvider)) {
  throw new Error('B5826/26 must not match a non-Chinese launch provider');
}
const wrongVehicle = { ...longMarch12, vehicle: 'Falcon 9 Block 5' };
if (warningRectanglePointsToLaunch(b5826, wrongVehicle)) {
  throw new Error('B5826/26 must not match a non-Long-March launch vehicle');
}
const maritime = { ...b5826, country: 'Maritime', source: 'MSI' };
if (warningRectanglePointsToLaunch(maritime, longMarch12) || relatedWarningRowsForLaunch(longMarch12, [maritime]).length) {
  throw new Error('Maritime/MSI data must remain outside the aviation fall-area matcher');
}

const a5624 = {
  country: 'CHINA',
  notam_id: 'A5624/26',
  qcode: 'QRDCA',
  from_utc: '2026-11-05T15:52:00Z',
  to_utc: '2026-11-05T16:16:00Z',
  lat: '18.379167',
  lon: '111.575',
  polygon: JSON.stringify([
    [18.45, 111.033333], [18.883333, 111.65],
    [18.316667, 112.133333], [17.866667, 111.483333],
  ]),
  raw: 'A5624/26 NOTAMN Q)ZJSA/QRDCA/IV/BO/W/000/999/1822N11134E032 A)ZJSA B)2611051552 C)2611051616 E) A TEMPORARY DANGER AREA ESTABLISHED BOUNDED BY: N182700E1110200-N185300E1113900-N181900E1120800-N175200E1112900, BACK TO START.',
};
const longMarch2c = {
  mission: 'Long March 2C | Unknown Payload',
  vehicle: 'Long March 2C',
  providerName: 'China Aerospace Science and Technology Corporation',
  ts: Date.parse('2026-11-05T16:00:00Z'),
  windowStartTs: Date.parse('2026-11-05T15:52:00Z'),
  windowEndTs: Date.parse('2026-11-05T16:16:00Z'),
  lat: 28.247059,
  lon: 102.029008,
};
if (warningRectanglePointsToLaunch(a5624, longMarch2c)) {
  throw new Error('A5624/26 must not be inferred as a Long March 2C match from time alone');
}
if (relatedWarningRowsForLaunch(longMarch2c, [a5624]).length !== 0) {
  throw new Error('A5624/26 should remain unassociated without validated downrange geometry');
}
if (relatedWarningRowsForLaunch(longMarch12, [a5624]).length !== 0) {
  throw new Error('A5624/26 must not cross-match the Long March 12 launch');
}
if (relatedWarningRowsForLaunch(longMarch2c, [b5826]).length !== 0) {
  throw new Error('B5826/26 must not cross-match the 16:00Z Long March 2C launch');
}

console.log('test_index_china_fall_area_matching.js passed');

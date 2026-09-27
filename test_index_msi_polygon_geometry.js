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
  'function normalizeDatelineCoordinates(',
  'function wrapDatelineLongitude(',
  'function sameDatelinePoint(',
  'function dedupeDatelinePoints(',
  'function clipDatelineRingAtLongitude(',
  'function compactDatelineSegments(',
  'function splitDatelinePath(',
  'function splitDatelineCoordinates(',
  'function haversineDistanceKm(',
  'function splitDisconnectedCoordinatePath(',
  'function isRouteGeometry(',
  'function coordinateSegmentsForRow(',
];

for (const signature of signatures) {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} not found`);
  global.eval(block);
}

// HYDROPAC 2761/26 is one area ring crossing the anti-meridian. Several of
// its legitimate oceanic edges exceed the old 900 km continuity threshold.
const hydropac2761 = [
  [23.916667, 144.616667], [26.116667, 151.233333],
  [27.683333, 156.333333], [29.416667, 165.05],
  [30.433333, 172.683333], [30.933333, -180.0],
  [31.183333, -175.0], [31.283333, -171.483333],
  [30.55, -161.25], [29.616667, -155.666667],
  [28.816667, -151.583333], [26.883333, -145.433333],
  [26.833333, -145.466667], [24.05, -154.533333],
  [23.766667, -156.183333], [24.866667, -159.316667],
  [27.633333, -167.816667], [29.416667, -175.0],
  [29.75, -176.583333], [29.816667, -180.0],
  [29.683333, 176.233333], [29.216667, 170.166667],
  [28.466667, 164.983333], [26.65, 156.25],
  [23.466667, 144.8],
];
const hydropacText = 'HYDROPAC 2761/26 HAZARDOUS OPERATIONS SPACE DEBRIS IN AREA BOUND BY';

if (isRouteGeometry(hydropacText)) {
  throw new Error('HYDROPAC 2761/26 must be classified as an area, not an open route');
}

const oldSegments = splitDisconnectedCoordinatePath(hydropac2761);
if (oldSegments.length <= 1) {
  throw new Error('Fixture no longer exercises the old long-edge split behavior');
}

const polygonSegments = coordinateSegmentsForRow(hydropac2761, hydropacText, true, true);
if (polygonSegments.length !== 2) {
  throw new Error(`Expected two anti-meridian render pieces for one area ring, got ${polygonSegments.length}`);
}
if (polygonSegments.some(segment => segment.length < 3)) {
  throw new Error(`Anti-meridian polygon pieces must remain closed-area candidates: ${JSON.stringify(polygonSegments)}`);
}

const routeSegments = coordinateSegmentsForRow(
  hydropac2761,
  'HYDROPAC OPEN ROUTE TRACK BETWEEN WAYPOINTS',
  true,
  true,
);
if (routeSegments.length <= 1) {
  throw new Error('Open maritime routes must retain distance-based discontinuity splitting');
}

const renderRowsBlock = extractFunctionBlock(html, 'function renderRows(');
const renderOnGlobeBlock = extractFunctionBlock(html, 'function renderOnGlobe(');
const kmlBlock = extractFunctionBlock(html, 'function buildKmlGeometryForRow(');
if (!renderRowsBlock || !renderOnGlobeBlock || !kmlBlock) {
  throw new Error('Could not inspect all MSI geometry render paths');
}
if (!/coordinateSegmentsForRow\(pts, rawText, isMaritime, true\)/.test(renderRowsBlock)) {
  throw new Error('Leaflet MSI rendering does not use the shared geometry segmentation rule');
}
if (!/coordinateSegmentsForRow\(points, r\.raw \|\| '', isMaritime, true\)/.test(renderOnGlobeBlock)) {
  throw new Error('Cesium MSI rendering does not use the shared geometry segmentation rule');
}
if (!/coordinateSegmentsForRow\(points, rawText, isMaritime, false\)/.test(kmlBlock)) {
  throw new Error('KML MSI rendering does not use the shared geometry segmentation rule');
}

console.log('test_index_msi_polygon_geometry.js passed');

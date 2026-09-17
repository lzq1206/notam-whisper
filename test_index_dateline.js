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
  'function expandDatelineWorldCopies(',
];
for (const signature of signatures) {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${signature} not found`);
  global.eval(block);
}

const b4610 = [
  [-58.25, -131.0],
  [-61.6875, -131.0],
  [-61.531667, -161.266944],
  [-62.6, 163.0],
  [-62.2, 163.0],
  [-59.71, 165.82],
  [-58.664444, -161.684167],
];

const canonical = splitDatelineCoordinates(b4610, true, false);
if (canonical.length !== 2) {
  throw new Error(`Expected two anti-meridian polygon rings, got ${canonical.length}`);
}
for (const ring of canonical) {
  if (ring.length < 3 || ring.some(point => point[1] < -180 || point[1] > 180)) {
    throw new Error(`Canonical ring is invalid: ${JSON.stringify(ring)}`);
  }
}

const compact = splitDatelineCoordinates(b4610, true, true);
if (compact.length !== 2) {
  throw new Error(`Expected two compact anti-meridian rings, got ${compact.length}`);
}
const compactLongitudes = compact.flat().map(point => point[1]);
if (Math.min(...compactLongitudes) < 150 || Math.max(...compactLongitudes) > 230) {
  throw new Error(`Compact rings were not kept in one local world copy: ${compactLongitudes}`);
}
if (Math.max(...compactLongitudes) - Math.min(...compactLongitudes) > 80) {
  throw new Error('Compact rings still span most of the world');
}

const worldCopies = expandDatelineWorldCopies(compact);
if (worldCopies.length !== compact.length * 2) {
  throw new Error(`Expected a westward world copy for each compact ring, got ${worldCopies.length}`);
}
const westCopy = worldCopies.slice(compact.length);
if (westCopy.some(ring => ring.some(point => point[1] > -130 || point[1] < -260))) {
  throw new Error(`Westward world copy is not positioned next to the canonical world: ${JSON.stringify(westCopy)}`);
}

const pathSegments = splitDatelineCoordinates([[0, 179], [0, -179]], false, true);
if (pathSegments.length !== 2 || pathSegments.some(segment => segment.length < 2)) {
  throw new Error(`Crossing path was not split at the date line: ${JSON.stringify(pathSegments)}`);
}

const ordinary = [[10, 120], [11, 121], [12, 120]];
const ordinarySegments = splitDatelineCoordinates(ordinary, true, true);
if (ordinarySegments.length !== 1 || JSON.stringify(ordinarySegments[0]) !== JSON.stringify(ordinary)) {
  throw new Error('An ordinary polygon should not be changed by anti-meridian handling');
}
if (expandDatelineWorldCopies(ordinarySegments).length !== 1) {
  throw new Error('An ordinary polygon should not receive duplicate world copies');
}

console.log('test_index_dateline.js passed');

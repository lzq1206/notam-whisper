#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

if (!/const \[minWrap, maxWrap\] = getVisibleWorldWrapRange\(\);[\s\S]*?for \(let wrap = minWrap - 1; wrap <= maxWrap \+ 1; wrap\+\+\)/.test(html)) {
  throw new Error('Solar overlay must render adjacent world-wrap copies');
}

if (!/const maskRings = nightMask\.nightBoundaryIsInner[\s\S]*?shiftLonPath\(nightMask\.daylightBoundary, lonOffset\)/.test(html)) {
  throw new Error('Night mask variants must be shifted with the same world-wrap offset');
}

if (!/isPointInSolarRing\(antiSubsolarPoint, daylightBoundary\)/.test(html)) {
  throw new Error('Night mask must classify the terminator side using the antipodal subsolar point');
}

if (!/L\.polyline\(shiftLonPath\(curve\.points, lonOffset\)/.test(html)) {
  throw new Error('Solar curves must be shifted with the same world-wrap offset');
}

console.log('Solar overlay world-wrap regression test passed.');

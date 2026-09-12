#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

if (!/function toCesiumDegreesArray\(points, preserveUnwrappedLongitude = false\)/.test(html)) {
  throw new Error('Cesium degree conversion must support unwrapped longitudes');
}

if (!/preserveUnwrappedLongitude \|\| Math\.abs\(lon\) <= 180/.test(html)) {
  throw new Error('Cesium degree conversion still drops date-line curve points');
}

if (!/buildSolarAltitudeCircle\(date, curve\.altitudeDeg\),\s*true\s*\)/.test(html)) {
  throw new Error('3D solar curves must preserve the continuous longitude path');
}

if (!/for \(let wrap = minWrap - 1; wrap <= maxWrap \+ 1; wrap\+\+\)/.test(html)) {
  throw new Error('2D adjacent world-wrap rendering regression was reintroduced');
}

console.log('Solar overlay 3D continuity regression test passed.');

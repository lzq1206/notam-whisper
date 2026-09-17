#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  if (start < 0) throw new Error(`Function not found: ${signature}`);
  const braceStart = src.indexOf('{', start);
  let depth = 0;
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}' && --depth === 0) return src.slice(start, i + 1);
  }
  throw new Error(`Unclosed function: ${signature}`);
}

const scriptStart = html.indexOf('<script>') + '<script>'.length;
const scriptEnd = html.indexOf('</script>', scriptStart);
const fakeModule = { exports: {} };
new Function('module', 'exports', html.slice(scriptStart, scriptEnd))(
  fakeModule,
  fakeModule.exports
);

const helperSource = [
  'function sunAltitudeRadians(',
  'function normalizeLongitude(',
  'function getSubsolarPoint(',
  'function buildSolarAltitudeCircle(',
  'function isPointInSolarRing(',
  'function buildNightMaskRings('
].map(signature => extractFunctionBlock(html, signature)).join('\n');

const helpers = new Function('SunCalc', `${helperSource}; return {
  sunAltitudeRadians,
  normalizeLongitude,
  getSubsolarPoint,
  buildSolarAltitudeCircle,
  isPointInSolarRing,
  buildNightMaskRings
};`)(fakeModule.exports);

function classify(date) {
  const subsolar = helpers.getSubsolarPoint(date);
  const boundary = helpers.buildSolarAltitudeCircle(date, 0);
  const mask = helpers.buildNightMaskRings(date);
  const antiSubsolar = [-subsolar.latDeg, subsolar.lonDeg + 180];
  return {
    subsolar,
    boundary,
    nightBoundaryIsInner: mask.nightBoundaryIsInner,
    outerCenterLon: mask.outerRing.reduce((sum, point) => sum + point[1], 0) / mask.outerRing.length,
    boundaryMeanLon: boundary.reduce((sum, point) => sum + point[1], 0) / boundary.length,
    antiAltitudeDeg: helpers.sunAltitudeRadians(
      date,
      antiSubsolar[0],
      helpers.normalizeLongitude(antiSubsolar[1])
    ) * 180 / Math.PI
  };
}

const a0095 = classify(new Date('2026-09-25T13:35:00Z'));
if (!a0095.nightBoundaryIsInner || a0095.antiAltitudeDeg > -89) {
  throw new Error(`A0095/26 midpoint must select the direct night ring: ${JSON.stringify(a0095)}`);
}

const northernSummer = classify(new Date('2026-06-21T12:00:00Z'));
if (northernSummer.nightBoundaryIsInner || northernSummer.antiAltitudeDeg > -89) {
  throw new Error(`Northern-summer mask must select the outer-minus-daylight-hole form: ${JSON.stringify(northernSummer)}`);
}
if (Math.abs(northernSummer.outerCenterLon - northernSummer.boundaryMeanLon) > 1e-9) {
  throw new Error(`Outer ring anchor must follow the unwrapped boundary mean: ${JSON.stringify(northernSummer)}`);
}

console.log('Solar night-side regression test passed.');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

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

const scriptStart = html.indexOf('<script>') + '<script>'.length;
const scriptEnd = html.indexOf('</script>', scriptStart);
const fakeModule = { exports: {} };
new Function('module', 'exports', html.slice(scriptStart, scriptEnd))(fakeModule, {});

const glowFunction = extractFunctionBlock(html, 'function isRocketCloudVisible(');
if (!glowFunction) throw new Error('isRocketCloudVisible function not found');
const isRocketCloudVisible = new Function(
  'SunCalc', `${glowFunction}; return isRocketCloudVisible;`
)(fakeModule.exports);

const f3436Glow = isRocketCloudVisible(
  '2026-09-15T21:55:00Z',
  '2026-09-15T22:36:00Z',
  -45.933333,
  121.429167,
  'F3436/26 NOTAMN'
);
if (f3436Glow) {
  throw new Error('F3436/26 should not glow when its midpoint is already in daylight');
}

const midpointTwilightGlow = isRocketCloudVisible(
  '2026-09-15T20:00:00Z',
  '2026-09-15T22:00:00Z',
  -45.933333,
  121.429167,
  'A1234/26'
);
if (!midpointTwilightGlow) {
  throw new Error('a NOTAM whose midpoint is in morning twilight should glow');
}

console.log('test_index_solar_glow_midpoint.js passed');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  if (start === -1) return null;
  const braceStart = src.indexOf('{', start);
  let depth = 0;
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}' && --depth === 0) return src.slice(start, i + 1);
  }
  return null;
}

const glowFunction = extractFunctionBlock(html, 'function isRocketCloudVisible(');
const iconFunction = extractFunctionBlock(html, 'function buildCesiumUpcomingLaunchIcon(');
if (!glowFunction || !iconFunction) throw new Error('required launch glow functions are missing');

const scriptStart = html.indexOf('<script', html.indexOf('<script') + 1) + '<script>'.length;
const scriptEnd = html.indexOf('</script>', scriptStart);
const fakeModule = { exports: {} };
new Function('module', 'exports', html.slice(scriptStart, scriptEnd))(fakeModule, fakeModule.exports);
const isRocketCloudVisible = new Function(
  'SunCalc', `${glowFunction}; return isRocketCloudVisible;`
)(fakeModule.exports);

global.btoa = value => Buffer.from(value, 'binary').toString('base64');
const buildCesiumUpcomingLaunchIcon = new Function(
  'btoa', 'unescape', 'encodeURIComponent', `${iconFunction}; return buildCesiumUpcomingLaunchIcon;`
)(global.btoa, global.unescape, global.encodeURIComponent);

const launches = [
  ['Long March 6A | Unknown Payload', '2026-10-23T10:15:00Z', 38.8583, 111.5802],
  ["Electron | We're Not Hawking Around (3x HawkEye 360)", '2026-10-22T07:00:00Z', -39.262833, 177.864469],
];
for (const [name, timestamp, lat, lon] of launches) {
  if (!isRocketCloudVisible(timestamp, timestamp, lat, lon, name)) {
    throw new Error(`${name} should satisfy the existing solar glow rule`);
  }
  const svg = Buffer.from(
    buildCesiumUpcomingLaunchIcon({ isLaunchGlow: true }, '#ff0000').split(',')[1],
    'base64'
  ).toString('utf8');
  if (!svg.includes('launch-icon-glow') || !svg.includes('feGaussianBlur')) {
    throw new Error(`${name} 3D icon is missing the white glow halo`);
  }
}

console.log('test_index_cesium_launch_glow.js passed');

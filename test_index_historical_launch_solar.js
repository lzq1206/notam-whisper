#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, message) {
  if (!re.test(html)) throw new Error(message);
}

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  if (start < 0) return null;
  const braceStart = src.indexOf('{', start);
  if (braceStart < 0) return null;
  let depth = 0;
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}' && --depth === 0) return src.slice(start, i + 1);
  }
  return null;
}

const curveDefinitions = extractFunctionBlock(html, 'function renderSolarOverlay2D(');
if (!curveDefinitions) throw new Error('2D solar overlay renderer is missing');
const curveDefinitionMatch = html.match(/const SOLAR_CURVE_DEFINITIONS = \[([\s\S]*?)\n    \];/);
if (!curveDefinitionMatch) throw new Error('solar curve definitions are missing');
const altitudes = [...curveDefinitionMatch[1].matchAll(/altitudeDeg:\s*(-?\d+)/g)].map(match => Number(match[1]));
if (JSON.stringify(altitudes) !== JSON.stringify([0, -6, -12, -18])) {
  throw new Error(`historical launch selection must reuse four solar curves, got ${JSON.stringify(altitudes)}`);
}

assertContains(
  /const historicalLaunch = \{[\s\S]*?isLaunchGlow:\s*isRocketCloudVisible\(d\.toISOString\(\),\s*d\.toISOString\(\),\s*lat,\s*lon\)/,
  'historical records must use the same midpoint glow classifier as upcoming launches'
);
assertContains(
  /const pIcon = buildUpcomingLaunchIcon\(historicalLaunch, '#22c55e'\)/,
  'historical 2D markers must reuse the upcoming launch icon glow builder'
);
assertContains(
  /marker\.on\('click',\s*\(\)\s*=>\s*selectLaunchForSolarOverlay\(historicalLaunch\)\)/,
  'historical 2D marker clicks must select the launch solar overlay'
);
assertContains(
  /pastLaunches\.push\(historicalLaunch\)/,
  'historical launch records must retain the solar/glow fields'
);

const renderOnGlobe = extractFunctionBlock(html, 'function renderOnGlobe(');
if (!renderOnGlobe) throw new Error('renderOnGlobe function is missing');
const pastBlockStart = renderOnGlobe.indexOf('if (pastLaunchToggle.checked)');
const upcomingBlockStart = renderOnGlobe.indexOf('// 1b. Render Upcoming Launches', pastBlockStart);
const pastBlock = pastBlockStart >= 0 && upcomingBlockStart > pastBlockStart
  ? renderOnGlobe.slice(pastBlockStart, upcomingBlockStart)
  : '';
if (!/buildCesiumUpcomingLaunchIcon\(launch, '#22c55e'\)/.test(pastBlock)) {
  throw new Error('historical 3D markers must reuse the glow-capable upcoming SVG builder');
}
if (!/addCesiumEntity\([\s\S]*?\},\s*launchSolarSelection\(launch\)\)/.test(pastBlock)) {
  throw new Error('historical 3D markers must carry launch solar selection payloads');
}

const iconFunction = extractFunctionBlock(html, 'function buildCesiumUpcomingLaunchIcon(');
if (!iconFunction) throw new Error('shared Cesium launch icon builder is missing');
const glowSvg = new Function(
  'btoa', 'unescape', 'encodeURIComponent',
  `${iconFunction}; return buildCesiumUpcomingLaunchIcon;`
)(value => Buffer.from(value, 'binary').toString('base64'), global.unescape, global.encodeURIComponent)({ isLaunchGlow: true }, '#22c55e');
const plainSvg = new Function(
  'btoa', 'unescape', 'encodeURIComponent',
  `${iconFunction}; return buildCesiumUpcomingLaunchIcon;`
)(value => Buffer.from(value, 'binary').toString('base64'), global.unescape, global.encodeURIComponent)({ isLaunchGlow: false }, '#22c55e');
const decode = value => Buffer.from(value.split(',')[1], 'base64').toString('utf8');
if (!decode(glowSvg).includes('launch-icon-glow') || !decode(glowSvg).includes('feGaussianBlur')) {
  throw new Error('shared glow-capable Cesium icon lost its halo markup');
}
if (decode(plainSvg).includes('launch-icon-glow')) {
  throw new Error('non-glow Cesium icon must not contain the halo markup');
}

assertContains(
  /function selectLaunchForSolarOverlay\(launch\)[\s\S]*?selectedNotamSolarState = selection;[\s\S]*?applySelectedNotamSolarOverlay\(\);/,
  'historical launch selection must use the shared four-curve solar overlay path'
);

console.log('test_index_historical_launch_solar.js passed');

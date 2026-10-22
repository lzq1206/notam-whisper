#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const url = 'https://exoplanet5.github.io/CZ-6A/';
const text = '火箭具体轨道详情与各地轨迹预测请参考 Mickey 网站';

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

const functionBlock = extractFunctionBlock(html, 'function buildLaunchOrbitDetailsLink(');
if (!functionBlock) throw new Error('mission-specific orbit detail helper is missing');
const buildLink = new Function(
  'LONG_MARCH_6A_ORBIT_DETAILS_URL', `${functionBlock}; return buildLaunchOrbitDetailsLink;`
)(url);

const longMarchLink = buildLink({ mission: 'Long March 6A | Unknown Payload' });
if (!longMarchLink.includes(`href="${url}"`)) throw new Error('Long March 6A link target is incorrect');
if (!longMarchLink.includes(text)) throw new Error('Long March 6A link text is incorrect');
if (!longMarchLink.includes('target="_blank"') || !longMarchLink.includes('noopener noreferrer')) {
  throw new Error('Long March 6A link must open safely in a new tab');
}
if (buildLink({ mission: 'Electron | We\'re Not Hawking Around (3x HawkEye 360)' }) !== '') {
  throw new Error('Mission-specific link must not appear on other launches');
}

const helperUses = (html.match(/buildLaunchOrbitDetailsLink\(/g) || []).length - 1;
if (helperUses !== 2) throw new Error(`Expected 2 click-detail uses (2D and 3D), found ${helperUses}`);

console.log('test_index_launch_orbit_detail_link.js passed');

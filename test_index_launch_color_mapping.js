#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const MS_PER_DAY = 86400000;

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  if (start === -1) return null;
  const braceStart = src.indexOf('{', start);
  if (braceStart === -1) return null;
  let depth = 0;
  for (let i = braceStart; i < src.length; i++) {
    const ch = src[i];
    if (ch === '{') depth++;
    if (ch === '}') {
      depth--;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return null;
}

const launchColorBlock = extractFunctionBlock(html, 'function launchColor(');
if (!launchColorBlock) {
  throw new Error('launchColor function not found');
}

if (!/return\s+colorByTime\(([^)]*)\)\s*;/.test(launchColorBlock)) {
  throw new Error('launchColor should delegate gradient mapping to colorByTime');
}

if (!/const minLaunchTs = now\.getTime\(\)\s*;/.test(html)) {
  throw new Error('Upcoming launch minLaunchTs should be anchored to now.getTime()');
}

if (!/const maxLaunchTs\s*=/.test(html)) {
  throw new Error('maxLaunchTs declaration not found');
}

if (!/Math\.max\(/.test(html)) {
  throw new Error('maxLaunchTs should use Math.max');
}

if (!new RegExp(`now\\.getTime\\(\\)\\s*\\+\\s*${MS_PER_DAY}`).test(html)) {
  throw new Error(`maxLaunchTs should include now.getTime() + ${MS_PER_DAY}`);
}

if (!/\.\.\.upcomingTimes/.test(html)) {
  throw new Error('maxLaunchTs should include ...upcomingTimes spread');
}

console.log('test_index_launch_color_mapping.js passed');

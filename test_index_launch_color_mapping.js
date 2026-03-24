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

if (!/return\s+colorByTime\(ts,\s*s,\s*e\)\s*;/.test(launchColorBlock)) {
  throw new Error('launchColor should delegate gradient mapping to colorByTime(ts, s, e)');
}

if (!/const minLaunchTs = now\.getTime\(\)\s*;/.test(html)) {
  throw new Error('Upcoming launch minLaunchTs should be anchored to now.getTime()');
}

if (!/const maxLaunchTs = upcomingTimes\.length \? Math\.max\(now\.getTime\(\) \+ 86400000,\s*\.\.\.upcomingTimes\) : now\.getTime\(\) \+ 86400000\s*;/.test(html)) {
  throw new Error('Upcoming launch maxLaunchTs should match NOTAM style max(now+1d, ...times)');
}

console.log('test_index_launch_color_mapping.js passed');

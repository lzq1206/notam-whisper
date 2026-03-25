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

if (!/return\s+colorByTime\(([^)]*)\)\s*;/.test(launchColorBlock)) {
  throw new Error('launchColor should delegate gradient mapping to colorByTime');
}

if (!/function getTimelineBounds\(/.test(html)) {
  throw new Error('getTimelineBounds helper not found');
}

if (!/const\s*\{\s*minTs\s*:\s*minLaunchTs\s*,\s*maxTs\s*:\s*maxLaunchTs\s*\}\s*=\s*getTimelineBounds\s*\(/.test(html)) {
  throw new Error('Upcoming launch bounds should include filteredDataRows and upcomingTimes');
}

if (!/const\s+launchTimes\s*=\s*upcomingLaunches\.map\(\s*l\s*=>\s*l\.ts\s*\)\.filter\(\s*Number\.isFinite\s*\)\s*;/.test(html)) {
  throw new Error('NOTAM rendering should gather launch timestamps for shared timeline bounds');
}

if (!/const\s*\{\s*minTs\s*,\s*maxTs\s*\}\s*=\s*getTimelineBounds\s*\(/.test(html)) {
  throw new Error('NOTAM timeline bounds should include launchTimes');
}

if (!/Math\.max\(\s*now\s*\+\s*86400000\s*,\s*\.\.\.mergedTimes\s*\)/.test(html)) {
  throw new Error('getTimelineBounds should include now + 86400000 safety window');
}

console.log('test_index_launch_color_mapping.js passed');

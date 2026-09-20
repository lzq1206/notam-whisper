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
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return null;
}

const parseDescriptionDateBlock = extractFunctionBlock(html, 'function parseDescriptionDate(');
const parseLaunchTimestampValueBlock = extractFunctionBlock(html, 'function parseLaunchTimestampValue(');
const timestampBlock = extractFunctionBlock(html, 'function launchTimestamp(');
const orderBlock = extractFunctionBlock(html, 'function orderUpcomingLaunchesFor2D(');
if (!parseDescriptionDateBlock || !parseLaunchTimestampValueBlock || !timestampBlock || !orderBlock) throw new Error('2D launch ordering helpers are missing');
global.eval(parseDescriptionDateBlock);
global.eval(parseLaunchTimestampValueBlock);
global.eval(timestampBlock);
global.eval(orderBlock);

const records = [
  { name: 'nearest', win_open: '2026-09-20T01:47:00Z' },
  { name: 'farthest', win_open: '2026-09-26T11:56:00Z' },
  { name: 'tbd', win_open: '' },
  { name: 'same-time', win_open: '2026-09-26T11:56:00Z' },
];
const orderedNames = orderUpcomingLaunchesFor2D(records).map(record => record.name);
const expected = ['tbd', 'farthest', 'same-time', 'nearest'];
if (JSON.stringify(orderedNames) !== JSON.stringify(expected)) {
  throw new Error(`unexpected overlap order: ${JSON.stringify(orderedNames)}`);
}

if (!/const\s+orderedList\s*=\s*orderUpcomingLaunchesFor2D\(list\)/.test(html)) {
  throw new Error('2D upcoming markers must use the deterministic overlap order');
}
if (!/zIndexOffset:\s*2000\s*\+\s*idx/.test(html)) {
  throw new Error('2D upcoming markers must assign an explicit stack rank');
}

console.log('2D upcoming launch overlap-order regression test passed.');

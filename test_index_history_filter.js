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

const applyFilterBlock = extractFunctionBlock(html, 'async function applyFilter(');
if (!applyFilterBlock) {
  throw new Error('async applyFilter function not found');
}

if (!/await\s+loadHistoryRange\(s,\s*e\)\s*;/.test(applyFilterBlock)) {
  throw new Error('applyFilter must await loadHistoryRange(s, e)');
}

const loadHistoryRangeBlock = extractFunctionBlock(html, 'async function loadHistoryRange(');
if (!loadHistoryRangeBlock) {
  throw new Error('loadHistoryRange function not found');
}

if (/updateLayers\(\);/.test(loadHistoryRangeBlock)) {
  throw new Error('loadHistoryRange should not call updateLayers directly');
}

console.log('test_index_history_filter.js passed');

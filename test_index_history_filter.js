#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

if (!/async function applyFilter\(\)/.test(html)) {
  throw new Error('applyFilter must be async');
}

if (!/await loadHistoryRange\(s, e\);/.test(html)) {
  throw new Error('applyFilter must await loadHistoryRange(s, e)');
}

const start = html.indexOf('async function loadHistoryRange(');
if (start === -1) {
  throw new Error('loadHistoryRange function not found');
}
const end = html.indexOf('\n    init();', start);
if (end === -1) {
  throw new Error('loadHistoryRange function end marker not found');
}
const loadHistoryRangeBlock = html.slice(start, end);
if (/updateLayers\(\);/.test(loadHistoryRangeBlock)) {
  throw new Error('loadHistoryRange should not call updateLayers directly');
}

console.log('test_index_history_filter.js passed');

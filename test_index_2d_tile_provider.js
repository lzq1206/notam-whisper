#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

const tileLayerUrlMatch = html.match(/L\.tileLayer\(\s*(['"])([^'"]*autonavi\.com\/appmaptile[^'"]*)\1/);
if (!tileLayerUrlMatch) {
  throw new Error('Expected default 2D L.tileLayer URL to use AutoNavi tile domain');
}

const tileUrl = tileLayerUrlMatch[2];
if (!/[?&]style=6(?:&|$)/.test(tileUrl)) {
  throw new Error('Expected AutoNavi tile URL to request satellite style=6');
}

if (!html.includes('worldCopyJump: true')) {
  throw new Error('Expected worldCopyJump to remain enabled for boundary-crossing display');
}

console.log('test_index_2d_tile_provider.js passed');

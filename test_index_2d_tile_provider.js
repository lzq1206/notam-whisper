#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

if (!html.includes("L.tileLayer('https://webrd02.is.autonavi.com/appmaptile?style=6&x={x}&y={y}&z={z}'")) {
  throw new Error('Expected default 2D tile layer to use AutoNavi satellite tiles');
}

if (!html.includes('worldCopyJump: true')) {
  throw new Error('Expected worldCopyJump to remain enabled for boundary-crossing display');
}

console.log('test_index_2d_tile_provider.js passed');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const expectedToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIzY2ZlZjYxZi1kOGM1LTRhN2MtOGRhNi1mMDBkMWEwNjZlYTkiLCJpZCI6NDA4NzUzLCJpYXQiOjE3NzQ0MDkwMTl9.StFh8-TIWbpATRQHRmTiHtxHGeRWFSc6SNsUcESHmhc';

if (!html.includes(`const CESIUM_ION_TOKEN = '${expectedToken}';`)) {
  throw new Error('Expected Cesium token constant was not found in index.html');
}

if (!html.includes('Cesium.Ion.defaultAccessToken = CESIUM_ION_TOKEN;')) {
  throw new Error('Cesium defaultAccessToken assignment not found in initCesium');
}

console.log('test_index_cesium_token.js passed');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

if (!/async function setCesiumBaseImagery\(\)/.test(html)) {
  throw new Error('Expected setCesiumBaseImagery helper in index.html');
}

if (!/const isIOS = \/iPad\|iPhone\|iPod\/\.test\(navigator\.userAgent\)\s*\|\|\s*\(navigator\.platform === 'MacIntel' && navigator\.maxTouchPoints > 1\);/.test(html)) {
  throw new Error('Expected iOS detection logic for Cesium imagery fallback');
}

if (!/const loaders = isIOS \?\s*\[\s*\(\) => Cesium\.createWorldImageryAsync\(\),[\s\S]*Cesium\.ArcGisMapServerImageryProvider\.fromUrl\(/.test(html)) {
  throw new Error('Expected iOS imagery loader order to prefer Cesium world imagery before ArcGIS');
}

if (!/setCesiumBaseImagery\(\);/.test(html)) {
  throw new Error('initCesium should call setCesiumBaseImagery');
}

console.log('test_index_cesium_ios_imagery.js passed');

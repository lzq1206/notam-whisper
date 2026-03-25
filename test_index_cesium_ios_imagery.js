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

const setCesiumBaseImageryBlock = extractFunctionBlock(html, 'async function setCesiumBaseImagery()');
if (!setCesiumBaseImageryBlock) {
  throw new Error('Expected setCesiumBaseImagery helper in index.html');
}

if (!setCesiumBaseImageryBlock.includes('/iPad|iPhone|iPod/.test(navigator.userAgent)')) {
  throw new Error('Expected iOS userAgent detection for Cesium imagery fallback');
}

if (!setCesiumBaseImageryBlock.includes("navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1")) {
  throw new Error('Expected iPadOS detection via MacIntel + touch points');
}

if (!setCesiumBaseImageryBlock.includes('const loaders = isIOS ?')) {
  throw new Error('Expected platform-aware imagery loader branching');
}

if (!setCesiumBaseImageryBlock.includes('Cesium.createWorldImageryAsync()')) {
  throw new Error('Expected Cesium world imagery provider in loader candidates');
}

if (!setCesiumBaseImageryBlock.includes('Cesium.ArcGisMapServerImageryProvider.fromUrl(')) {
  throw new Error('Expected ArcGIS imagery provider in loader candidates');
}

if (!setCesiumBaseImageryBlock.includes('new Cesium.OpenStreetMapImageryProvider(')) {
  throw new Error('Expected OSM imagery provider fallback in loader candidates');
}

if (!/setCesiumBaseImagery\(\);/.test(html)) {
  throw new Error('initCesium should call setCesiumBaseImagery');
}

if (!setCesiumBaseImageryBlock.includes('all base imagery providers are unavailable')) {
  throw new Error('Expected explicit error log when all imagery providers fail');
}

if (!setCesiumBaseImageryBlock.includes('Cesium Imagery provider #')) {
  throw new Error('Expected per-provider error logging for imagery fallback attempts');
}

console.log('test_index_cesium_ios_imagery.js passed');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, msg) {
  if (!re.test(html)) throw new Error(msg);
}

assertContains(/href="https:\/\/lzq1206\.github\.io\/OrbitWhisper\/"/, 'OrbitWhisper link href should exist');
assertContains(/>在轨卫星风险检测系统<\//, 'OrbitWhisper link text should exist');
assertContains(/style="color:#fff; text-decoration:none;"/, 'OrbitWhisper link should use white text style');

console.log('test_index_orbitwhisper_link.js passed');

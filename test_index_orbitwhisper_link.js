#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, msg) {
  if (!re.test(html)) throw new Error(msg);
}

function getCssBlock(selectorRegex) {
  const match = html.match(selectorRegex);
  if (!match) throw new Error('OrbitWhisper link CSS selector block should exist');
  if (typeof match[1] !== 'string') throw new Error('OrbitWhisper link CSS selector block has unexpected format');
  return match[1];
}

assertContains(/href="https:\/\/lzq1206\.github\.io\/OrbitWhisper\/"/, 'OrbitWhisper link href should exist');
assertContains(/>在轨卫星风险检测系统<\//, 'OrbitWhisper link text should exist');
assertContains(/aria-label="在轨卫星风险检测系统（新窗口打开）"/, 'OrbitWhisper link should include aria-label for new window behavior');
const footerLinkCss = getCssBlock(/\.panel-footer-link[\s\S]*?a\s*\{([^}]*)\}/);
if (!/color:\s*#fff/.test(footerLinkCss)) throw new Error('OrbitWhisper link CSS should set white color');
if (!/text-decoration:\s*underline/.test(footerLinkCss)) throw new Error('OrbitWhisper link CSS should set underline');
const footerContainerCss = getCssBlock(/\.panel-footer-link\s*\{([^}]*)\}/);
if (!/border-top:\s*1px\s+solid\s+#eee/.test(footerContainerCss)) throw new Error('OrbitWhisper footer should have top separator line');
if (!/font-size:\s*13px/.test(footerContainerCss)) throw new Error('OrbitWhisper link area should match launch title size');

console.log('test_index_orbitwhisper_link.js passed');

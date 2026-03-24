#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, msg) {
  if (!re.test(html)) throw new Error(msg);
}

assertContains(/id="downloadKmlBtn"/, 'download KML button should exist in HTML');
assertContains(/function\s+generateCurrentViewKml\s*\(/, 'generateCurrentViewKml function should exist');
assertContains(/function\s+downloadCurrentKml\s*\(/, 'downloadCurrentKml function should exist');
assertContains(/downloadKmlBtn\.addEventListener\(\s*'click'\s*,\s*downloadCurrentKml\s*\)/, 'download button click handler should be wired');

assertContains(/类型:\s*航空 NOTAM/, 'KML description should include NOTAM type text');
assertContains(/类型:\s*海事警告 \(MSI\)/, 'KML description should include MSI type text');
assertContains(/类型:\s*即将发射火箭/, 'KML description should include launch type text');
assertContains(/原文:/, 'KML should include NOTAM/MSI raw text label');
assertContains(/详情:/, 'KML should include launch details label');
assertContains(/function\s+buildKmlGeometryForRow\s*\(/, 'KML export should build geometry for NOTAM/MSI');
assertContains(/<Polygon>|<LineString>|<MultiGeometry>/, 'KML export should include non-point geometry output for outlines');

console.log('test_index_kml_download.js passed');

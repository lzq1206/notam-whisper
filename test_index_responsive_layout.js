#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, message) {
  if (!re.test(html)) throw new Error(message);
}

assertContains(/class="date-range"[\s\S]*id="startDate"[\s\S]*id="endDate"/, 'start and end date controls should share one row');
assertContains(/class="row control-row"[\s\S]*id="applyBtn"[\s\S]*id="resetBtn"[\s\S]*id="downloadKmlBtn"/, 'filter, reset, and KML buttons should share one row');
assertContains(/class="legend-launches-primary"[\s\S]*launchPastLegend[\s\S]*launchUpcomingNearLegend[\s\S]*launchUpcomingFarLegend/, 'primary launch legend items should share one row');
assertContains(/\.past-launch-range label > span\s*\{[\s\S]*display:\s*block/, 'historical date labels should sit above their inputs');
assertContains(/\.past-launch-range input\s*\{[\s\S]*width:\s*calc\(100%\s*-\s*4px\)/, 'historical date inputs should be horizontally compressed');
assertContains(/\.date-range input\s*\{[\s\S]*width:\s*calc\(100%\s*-\s*4px\)[\s\S]*padding:\s*3px\s+4px[\s\S]*font-size:\s*10px/, 'primary date inputs should match historical date input sizing');
assertContains(/\.control-row #resetBtn\s*\{[\s\S]*flex:\s*0\s+0\s+auto/, 'reset button should stay close to its text width');
assertContains(/\.control-row #downloadKmlBtn\s*\{[\s\S]*flex:\s*1\.8\s+1\s+0/, 'KML download button should receive extra width');
assertContains(/\.view-toggle\s*\{[\s\S]*bottom:\s*54px/, '3D toggle should be raised above the bottom attribution');
assertContains(/@media\s*\(max-width:\s*600px\)[\s\S]*\.view-toggle\s*\{[\s\S]*bottom:\s*62px/, '3D toggle should be raised further on mobile');

console.log('test_index_responsive_layout.js passed');

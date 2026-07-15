#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, message) {
  if (!re.test(html)) throw new Error(message);
}

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  if (start === -1) return null;
  const braceStart = src.indexOf('{', start);
  let depth = 0;
  for (let i = braceStart; i < src.length; i++) {
    if (src[i] === '{') depth++;
    if (src[i] === '}' && --depth === 0) return src.slice(start, i + 1);
  }
  return null;
}

assertContains(/lMarker\.on\('click',\s*\(\)\s*=>\s*selectLaunchForSolarOverlay\(launchRecord\)\)/,
  '2D upcoming launch click must select the launch time for the solar overlay');
assertContains(/addCesiumEntity\([\s\S]*?description:\s*`<b>\$\{launch\.mission\}[\s\S]*?launchSolarSelection\(launch\)\)/,
  '3D upcoming launch entity must carry a launch solar selection');

const launchSelection = extractFunctionBlock(html, 'function launchSolarSelection(');
if (!launchSelection || !/midpoint:\s*new Date\(timestamp\)/.test(launchSelection)) {
  throw new Error('launch solar selection must use the exact launch timestamp');
}

assertContains(/createPane\('msiPane'\)[\s\S]*?zIndex\s*=\s*430/,
  'MSI pane must have an explicit lower z-index');
assertContains(/createPane\('notamPane'\)[\s\S]*?zIndex\s*=\s*440/,
  'NOTAM pane must have an explicit higher z-index');
assertContains(/const warningPane = isMaritime \? 'msiPane' : 'notamPane'/,
  'warning geometry must be routed to the correct pane');

const defaultRange = extractFunctionBlock(html, 'function defaultPastLaunchDateRange(');
if (!defaultRange) throw new Error('default historical launch date range helper is missing');
global.localIsoDate = d => {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};
global.eval(defaultRange);
const julyRange = defaultPastLaunchDateRange(new Date(2026, 6, 15, 12));
if (julyRange.start !== '2026-06-15' || julyRange.end !== '2026-07-15') {
  throw new Error(`unexpected default range: ${JSON.stringify(julyRange)}`);
}
const marchRange = defaultPastLaunchDateRange(new Date(2026, 2, 31, 12));
if (marchRange.start !== '2026-02-28' || marchRange.end !== '2026-03-31') {
  throw new Error(`month-end range is not clamped: ${JSON.stringify(marchRange)}`);
}
assertContains(/pastLaunchStartDateEl\.value = defaultPastLaunchRange\.start/,
  'historical start input must initialize from the one-month default');
assertContains(/pastLaunchEndDateEl\.value = defaultPastLaunchRange\.end/,
  'historical end input must initialize to today');

assertContains(/\.panel\s*\{[\s\S]*?width:\s*min\(354px,\s*calc\(100vw - 20px\)\)/,
  'panel width must remain fixed while history details expand');
assertContains(/\.launch-history-list\s*\{[\s\S]*?max-width:\s*100%[\s\S]*?overflow-x:\s*hidden/,
  'history list must stay within the fixed panel width');

console.log('test_index_launch_interaction_ui.js passed');

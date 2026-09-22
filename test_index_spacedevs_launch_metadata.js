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
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}' && --depth === 0) return src.slice(start, i + 1);
  }
  return null;
}

const functionNames = [
  'escapeHtml',
  'parseDescriptionDate',
  'parseLaunchTimestampValue',
  'launchTimestamp',
  'launchWindowStartTimestamp',
  'launchWindowEndTimestamp',
  'launchNameKey',
  'launchNamesMatch',
  'safeLaunchHttpUrl',
  'launchImageUrl',
  'launchImageCredit',
  'launchMetadataMatchScore',
  'enrichLaunchWithSpaceDevs',
  'enrichUpcomingLaunchesWithSpaceDevs',
];

global.SPACEDEVS_UPCOMING_URL = 'https://ll.thespacedevs.com/2.3.0/launches/';
global.t = key => key;
for (const name of functionNames) {
  const block = extractFunctionBlock(html, `function ${name}(`);
  if (!block) throw new Error(`${name} is missing`);
  global.eval(block);
}

const target = {
  name: 'Long March 8A | Unknown Payload',
  t0: '2026-09-23T13:30:00Z',
  pad: { location: { name: 'Wenchang Space Launch Site' } },
};
const metadata = {
  id: 'space-devs-1',
  url: 'https://ll.thespacedevs.com/2.3.0/launches/space-devs-1/',
  name: 'Long March 8A | Unknown Payload',
  net: '2026-09-23T13:30:00Z',
  window_start: '2026-09-23T13:24:00Z',
  window_end: '2026-09-23T13:49:00Z',
  image: {
    image_url: 'https://thespacedevs-prod.nyc3.digitaloceanspaces.com/media/images/test-launch.jpeg',
    thumbnail_url: 'https://thespacedevs-prod.nyc3.digitaloceanspaces.com/media/images/test-launch-thumbnail.jpeg',
    credit: 'Test credit',
  },
};

const enriched = enrichLaunchWithSpaceDevs(target, [metadata]);
if (enriched.image?.image_url !== metadata.image.image_url) {
  throw new Error('Space Devs image metadata was not merged into the launch record');
}
if (enriched.window_start !== metadata.window_start || enriched.window_end !== metadata.window_end) {
  throw new Error('Space Devs launch window was not merged into the launch record');
}
if (enriched.spaceDevsUrl !== metadata.url) {
  throw new Error('Space Devs detail URL was not retained');
}
if (launchImageUrl(enriched) !== metadata.image.image_url || launchImageCredit(enriched) !== 'Test credit') {
  throw new Error('launch image helper did not prefer the full image URL and credit');
}
if (safeLaunchHttpUrl('javascript:alert(1)') !== '') {
  throw new Error('unsafe launch image URL was not rejected');
}

if (!/const\s+SPACEDEVS_UPCOMING_URL\s*=\s*['"]https:\/\/ll\.thespacedevs\.com\/2\.3\.0\/launches\//.test(html)) {
  throw new Error('Launch Library 2 endpoint is missing');
}
if (!/mode:\s*'list'/.test(html) || !/window_end__gte:\s*new Date\(nowMs\)\.toISOString\(\)/.test(html)) {
  throw new Error('Space Devs request must use list mode and an end-of-window lower bound');
}
if (!/buildLaunchImageMarkup\(launchRecord,\s*missionName\)/.test(html) ||
  !/buildLaunchImageMarkup\(launch,\s*launch\.mission/.test(html)) {
  throw new Error('2D and 3D launch details must render the launch image when available');
}

console.log('Launch Library 2 metadata regression test passed.');

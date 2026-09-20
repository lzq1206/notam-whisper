#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function extractFunctionBlock(src, signature) {
  const start = src.indexOf(signature);
  if (start === -1) return null;
  const signatureEnd = src.indexOf(')', start);
  const braceStart = src.indexOf('{', signatureEnd);
  if (braceStart === -1) return null;
  let depth = 0;
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return null;
}

const functionNames = [
  'parseDescriptionDate',
  'parseLaunchTimestampValue',
  'launchTimestamp',
  'normalizeLocationKey',
  'launchLocationObject',
  'coordsForRllLoc',
  'launchCoordinates',
];
for (const name of functionNames) {
  const block = extractFunctionBlock(html, `function ${name}(`);
  if (!block) throw new Error(`${name} is missing`);
  global.eval(block);
}

// Minimal copies of the live page's lookup tables keep this test independent
// of browser globals while exercising the current RocketLaunch.Live schema.
global.RLL_LOC_TO_SITE_ABBR = {
  'vandenberg-sfb': 'VSFB',
  'jiuquan-satellite-launch-center': 'JSLC',
  'wenchang-satellite-launch-center': 'WSLC',
  'taiyuan-satellite-launch-center': 'TSLC',
  'rocket-lab-launch-complex-mahia-peninsula': 'LC-1',
};
global.RLL_LOC_COORDS = {
  'wenchang-satellite-launch-center': { lat: 19.615, lon: 110.951 },
  'taiyuan-satellite-launch-center': { lat: 37.5, lon: 112.6 },
};
global.launchSites = {
  VSFB: { lat: 34.742, lon: -120.572 },
  JSLC: { lat: 40.960, lon: 100.298 },
  WSLC: { lat: 19.614, lon: 110.951 },
  TSLC: { lat: 38.849, lon: 111.608 },
  'LC-1': { lat: -39.261, lon: 177.865 },
};
global.launchSitesByName = {};

const livePayload = [
  {
    name: 'Starlink (15-27)',
    t0: '2026-09-20T01:47Z',
    win_open: null,
    pad: { location: { name: 'Vandenberg SFB', slug: 'vandenberg-sfb' } },
  },
  {
    name: 'TBD',
    t0: '2026-09-23T13:30Z',
    win_open: null,
    pad: { location: { name: 'Wenchang Satellite Launch Center', slug: 'wenchang-satellite-launch-center' } },
  },
  {
    name: 'TBD',
    t0: '2026-09-24T08:45Z',
    win_open: null,
    pad: { location: { name: 'Taiyuan Satellite Launch Center', slug: 'taiyuan-satellite-launch-center' } },
  },
  {
    name: 'StriX Launch 13',
    t0: null,
    win_open: '2026-09-26T00:15Z',
    pad: { location: { name: 'Rocket Lab Launch Complex, Mahia Peninsula', slug: 'rocket-lab-launch-complex-mahia-peninsula' } },
  },
];

const expectedTimes = [
  '2026-09-20T01:47:00.000Z',
  '2026-09-23T13:30:00.000Z',
  '2026-09-24T08:45:00.000Z',
  '2026-09-26T00:15:00.000Z',
];
livePayload.forEach((launch, index) => {
  const timestamp = launchTimestamp(launch);
  if (new Date(timestamp).toISOString() !== expectedTimes[index]) {
    throw new Error(`unexpected launch timestamp for ${launch.name}: ${timestamp}`);
  }
  const coordinates = launchCoordinates(launch);
  if (!coordinates || !Number.isFinite(coordinates.lat) || !Number.isFinite(coordinates.lon)) {
    throw new Error(`current RLL record has no coordinates: ${launch.name}`);
  }
});

if (launchTimestamp({ t0: '2026-09-20T04:00Z', win_open: '2026-09-21T04:00Z' }) !== Date.parse('2026-09-20T04:00Z')) {
  throw new Error('RocketLaunch.Live t0 must take precedence over win_open');
}

const stringLocation = {
  t0: '2026-09-27T00:00Z',
  pad: { location: 'Wenchang Satellite Launch Center' },
};
if (!launchCoordinates(stringLocation)) {
  throw new Error('legacy string-form RLL locations should remain resolvable');
}

console.log('RocketLaunch.Live schema regression test passed.');

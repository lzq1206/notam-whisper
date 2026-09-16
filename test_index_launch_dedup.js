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
    if (src[i] === '{') depth++;
    if (src[i] === '}') {
      depth--;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return null;
}

const blocks = [
  ['normalizeLocationKey', 'function normalizeLocationKey('],
  ['launchTimestampBlock', 'function launchTimestamp('],
  ['launchNameKeyBlock', 'function launchNameKey('],
  ['launchLocationKeysBlock', 'function launchLocationKeys('],
  ['launchNamesMatchBlock', 'function launchNamesMatch('],
  ['launchLocationsMatchBlock', 'function launchLocationsMatch('],
  ['launchRecordsMatchBlock', 'function launchRecordsMatch('],
  ['mergeUpcomingLaunchesBlock', 'function mergeUpcomingLaunches('],
];
for (const [label, signature] of blocks) {
  const block = extractFunctionBlock(html, signature);
  if (!block) throw new Error(`${label} is missing`);
  global.eval(block);
}

const localProgress = {
  name: 'Progress MS-35 (96P)',
  win_open: '2026-09-16T13:33:00Z',
  pad: { location: { name: 'Baikonur Cosmodrome' } },
  sourceLabel: 'Data by local schedule',
};
const apiProgress = {
  name: 'Progress MS-35',
  win_open: '2026-09-16T13:33:00Z',
  pad: { location: { name: 'Baikonur Cosmodrome' } },
};
const mergedProgress = mergeUpcomingLaunches([localProgress], [apiProgress]);
if (mergedProgress.length !== 1 || mergedProgress[0] !== apiProgress) {
  throw new Error('API Progress MS-35 must suppress its matching local fallback record');
}

const localUnique = {
  name: 'Shanghai Sea Launch',
  win_open: '2026-09-20T21:55:00Z',
  pad: { location: { name: 'Shanghai Sea Launch' } },
};
const mergedUnique = mergeUpcomingLaunches([localUnique], []);
if (mergedUnique.length !== 1 || mergedUnique[0] !== localUnique) {
  throw new Error('a local launch absent from the API must remain available');
}

const localRescheduled = {
  name: 'USSF-259',
  win_open: '2026-09-16T01:00:00Z',
  pad: { location: { name: 'Vandenberg Space Force Base' } },
};
const apiRescheduled = {
  name: 'USSF-259',
  win_open: '2026-09-17T01:00:00Z',
  pad: { location: { name: 'Vandenberg SFB' } },
};
const mergedRescheduled = mergeUpcomingLaunches([localRescheduled], [apiRescheduled]);
if (mergedRescheduled.length !== 2) {
  throw new Error('different site labels must not collapse launches without a matching location key');
}

if (!/const\s+list\s*=\s*mergeUpcomingLaunches\([\s\S]*?filterFutureLaunches\(localUpcoming[\s\S]*?filterFutureLaunches\(remoteUpcoming/.test(html)) {
  throw new Error('API and local launches must be filtered and merged through the deduplication layer');
}
if (!/return\s+\[\.\.\.remote,\s*\.\.\.localOnly\];/.test(html)) {
  throw new Error('the API launch layer must remain authoritative in the merged result');
}

console.log('test_index_launch_dedup.js passed');

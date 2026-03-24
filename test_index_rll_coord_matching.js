#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

if (!/const RLL_LOC_TO_SITE_ABBR = \{/.test(html)) {
  throw new Error('RLL_LOC_TO_SITE_ABBR mapping not found');
}

if (!/'wenchang-space-launch-site':\s*'WSLC'/.test(html)) {
  throw new Error('Wenchang slug should map to WSLC');
}

if (!/'jiuquan-satellite-launch-center':\s*'JSLC'/.test(html)) {
  throw new Error('Jiuquan slug should map to JSLC');
}

if (!/if \(mappedAbbr && launchSites\[mappedAbbr\]\)\s*\{\s*return \{ lat: launchSites\[mappedAbbr\]\.lat, lon: launchSites\[mappedAbbr\]\.lon \};\s*\}/s.test(html)) {
  throw new Error('coordsForRllLoc should prioritize launchSites coordinates through slug-abbr mapping');
}

if (!/const normalizedName = normalizeLocationKey\(name\);/.test(html)) {
  throw new Error('coordsForRllLoc should normalize location names');
}

if (!/if \(normalizedName && launchSitesByName\[normalizedName\]\)\s*\{\s*return launchSitesByName\[normalizedName\];\s*\}/s.test(html)) {
  throw new Error('coordsForRllLoc should look up launch site coordinates by normalized name');
}

if (!/if \(loc\) \{\s*const coords = coordsForRllLoc\(loc\);\s*if \(coords\) \{ lat = coords\.lat; lon = coords\.lon; \}\s*\}/s.test(html)) {
  throw new Error('Upcoming launch loop should always attempt location-matched coordinates');
}

console.log('test_index_rll_coord_matching.js passed');

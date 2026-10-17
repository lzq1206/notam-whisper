#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

if (!/const RLL_LOC_TO_SITE_ABBR\s*=\s*\{/.test(html)) {
  throw new Error('RLL_LOC_TO_SITE_ABBR mapping not found');
}

if (!/'wenchang-space-launch-site':\s*'WSLC'/.test(html)) {
  throw new Error('Wenchang slug should map to WSLC');
}

if (!/'rizhao-offshore-launch-location':\s*'RSLA'/.test(html) ||
    !/'rizhao offshore launch location':\s*'RSLA'/.test(html)) {
  throw new Error('Rizhao offshore pad metadata should map to the distinct RSLA site');
}

if (!/'wenchang-satellite-launch-center':\s*'WSLC'/.test(html) ||
    !/'taiyuan-satellite-launch-center':\s*'TSLC'/.test(html)) {
  throw new Error('current RocketLaunch.Live satellite-center slugs should map to launch sites');
}

if (!/'jiuquan-satellite-launch-center':\s*'JSLC'/.test(html)) {
  throw new Error('Jiuquan slug should map to JSLC');
}

if (!/const mappedAbbr = RLL_LOC_TO_SITE_ABBR\[slug\]\s*\|\|\s*nameAliasAbbr;/.test(html)) {
  throw new Error('coordsForRllLoc should resolve explicit pad-name aliases as well as slugs');
}

if (!/mappedAbbr && launchSites\[mappedAbbr\]/.test(html)) {
  throw new Error('coordsForRllLoc should gate mapped lookup on existing launch site');
}

if (!/launchSites\[mappedAbbr\]\.lat/.test(html) || !/launchSites\[mappedAbbr\]\.lon/.test(html)) {
  throw new Error('coordsForRllLoc should return mapped launch site latitude and longitude');
}

if (!/function coordsForRllLoc\(padLoc, options = \{\}\)/.test(html)) {
  throw new Error('coordsForRllLoc should accept options for selective fallbacks');
}

if (!/const siteOnly = !!options\.siteOnly;/.test(html)) {
  throw new Error('coordsForRllLoc should support siteOnly mode');
}

if (!/if \(siteOnly\) return null;/.test(html)) {
  throw new Error('siteOnly mode should skip hardcoded coordinate fallback');
}

if (!/const normalizedName = normalizeLocationKey\(name\);/.test(html)) {
  throw new Error('coordsForRllLoc should normalize location names');
}

if (!/if\s*\(\s*normalizedName && launchSitesByName\[normalizedName\]\s*\)/.test(html) ||
    !/return launchSitesByName\[normalizedName\];/.test(html)) {
  throw new Error('coordsForRllLoc should look up launch site coordinates by normalized name');
}

if (!/function launchLocationObject\(launch\)/.test(html) || !/function launchCoordinates\(launch\)/.test(html)) {
  throw new Error('Upcoming launch loop should normalize current and legacy RLL location schemas');
}

if (!/const padSiteCoordinates = pad\.name \? coordsForRllLoc\(pad\.name, \{ siteOnly: true \}\) : null;[\s\S]*?if \(padSiteCoordinates\) return padSiteCoordinates;/.test(html)) {
  throw new Error('launchCoordinates should prefer a specific Rizhao pad over its Haiyang parent spaceport');
}

if (!/const loc = launchLocationObject\(l\)/.test(html) || !/const coords = launchCoordinates\(l\)/.test(html)) {
  throw new Error('Upcoming launch loop should resolve coordinates through the normalized launch schema');
}

if (!/if \(!coords\) return;/.test(html)) {
  throw new Error('Upcoming launch loop should skip only launches without resolvable coordinates');
}

if (!/'taiyuan-satellite-launch-center':\s*\{\s*lat:\s*37\.5,\s*lon:\s*112\.6\s*\}/.test(html) ||
    !/'wenchang-satellite-launch-center':\s*\{\s*lat:\s*19\.615,\s*lon:\s*110\.951\s*\}/.test(html)) {
  throw new Error('current satellite-center slugs should have hardcoded coordinate fallbacks');
}

if (!/if\s*\(\s*!launchSitesByName\[normalizedName\]\s*\)\s*\{[\s\S]*?launchSitesByName\[normalizedName\]\s*=\s*\{\s*lat\s*,\s*lon\s*\}\s*;\s*\}/s.test(html)) {
  throw new Error('launchSitesByName should keep first normalized site match and avoid silent overwrite');
}

console.log('test_index_rll_coord_matching.js passed');

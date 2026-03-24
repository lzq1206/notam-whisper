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

if (!/'jiuquan-satellite-launch-center':\s*'JSLC'/.test(html)) {
  throw new Error('Jiuquan slug should map to JSLC');
}

if (!/const mappedAbbr = RLL_LOC_TO_SITE_ABBR\[slug\];/.test(html)) {
  throw new Error('coordsForRllLoc should resolve mappedAbbr from slug');
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

if (!/const hasApiCoords = isFinite\(parseFloat\(lat\)\) && isFinite\(parseFloat\(lon\)\);/.test(html)) {
  throw new Error('Upcoming launch loop should detect API coordinates robustly');
}

if (!/coordsForRllLoc\(loc, \{ siteOnly: hasApiCoords \}\)/.test(html)) {
  throw new Error('Upcoming launch loop should avoid hardcoded fallback when API coords exist');
}

if (!/if \(!isFinite\(parseFloat\(lat\)\) \|\| !isFinite\(parseFloat\(lon\)\)\) return;/.test(html)) {
  throw new Error('Upcoming launch loop should validate both latitude and longitude');
}

if (!/if \(!hasApiCoords && coords\) \{ lat = coords\.lat; lon = coords\.lon; \}/.test(html)) {
  throw new Error('Upcoming launch loop should only override coordinates when API coords are missing');
}

if (!/if\s*\(\s*!launchSitesByName\[normalizedName\]\s*\)\s*\{[\s\S]*?launchSitesByName\[normalizedName\]\s*=\s*\{\s*lat\s*,\s*lon\s*\}\s*;\s*\}/s.test(html)) {
  throw new Error('launchSitesByName should keep first normalized site match and avoid silent overwrite');
}

console.log('test_index_rll_coord_matching.js passed');

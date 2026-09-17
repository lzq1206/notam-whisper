#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const schedule = fs.readFileSync(path.join(__dirname, 'upcoming_launches.csv'), 'utf8');

if (!/const\s+RLL_UPCOMING_URL\s*=\s*['"]https:\/\/fdo\.rocketlaunch\.live\/json\/launches\/next\/5['"]/.test(html)) {
  throw new Error('The existing RocketLaunch.Live upcoming API endpoint must remain unchanged');
}

if (!/const\s+rllResp\s*=\s*await\s+fetch\(RLL_UPCOMING_URL,\s*\{\s*cache:\s*['"]no-store['"]\s*\}\)/.test(html)) {
  throw new Error('Upcoming launches must continue to come from the RocketLaunch.Live API');
}

if (!/remoteUpcoming\s*=\s*rllData\.result\s*\|\|\s*\[\]/.test(html)) {
  throw new Error('RocketLaunch.Live API results must continue through the existing merge path');
}

if (!schedule.includes('2026 SEP 28 1215,Starbase,25.997,-97.156,Starship,Starship Orbital Flight 1 (Starship Flight 14)')) {
  throw new Error('Starship Orbital Flight 1 fallback schedule entry is missing');
}

if (!schedule.includes('https://www.rocketlaunch.live/launch/starship-flight-1')) {
  throw new Error('Starship fallback must retain the supplied RocketLaunch.Live mission URL');
}

console.log('test_index_rll_upcoming_window.js passed');

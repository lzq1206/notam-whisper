#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Missing function ${name}`);
  const brace = html.indexOf('{', start);
  let depth = 0;
  for (let i = brace; i < html.length; i++) {
    if (html[i] === '{') depth++;
    else if (html[i] === '}' && --depth === 0) return html.slice(start, i + 1);
  }
  throw new Error(`Unclosed function ${name}`);
}

const launchLocationObject = new Function(`return (${extractFunction('launchLocationObject')})`)();
const coordsForRllLoc = () => null;
const launchCoordinates = new Function('launchLocationObject', 'coordsForRllLoc', `return (${extractFunction('launchCoordinates')})`)(launchLocationObject, coordsForRllLoc);

const liveSpaceDevsRecord = {
  pad: {
    name: 'Rizhao offshore launch location',
    latitude: 35.391667,
    longitude: 119.598667,
    location: {
      name: 'Haiyang Oriental Spaceport',
      latitude: 36.676794,
      longitude: 121.235103
    }
  }
};
const actual = launchCoordinates(liveSpaceDevsRecord);
if (actual?.lat !== 35.391667 || actual?.lon !== 119.598667) {
  throw new Error(`Pad-specific Rizhao coordinates must beat Haiyang parent coordinates; got ${JSON.stringify(actual)}`);
}

const parentFallbackRecord = {
  pad: {
    name: 'Unmapped launch pad',
    location: { name: 'Parent site', latitude: 12.5, longitude: 34.5 }
  }
};
const fallback = launchCoordinates(parentFallbackRecord);
if (fallback?.lat !== 12.5 || fallback?.lon !== 34.5) {
  throw new Error(`Parent location coordinates should remain a fallback; got ${JSON.stringify(fallback)}`);
}

console.log('test_index_launch_pad_coordinates.js passed');

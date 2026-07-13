#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, msg) {
  if (!re.test(html)) throw new Error(msg);
}

assertContains(/id="launchHistorySummary"/, 'launch history summary control should exist');
assertContains(/id="launchHistoryList"/, 'launch history list should exist');
assertContains(/<input type="checkbox" id="pastLaunchToggle" \/>/, 'historical launch toggle should exist and default to unchecked');
assertContains(/launch_history_summary:\s*'查看全部历史发射记录'/, 'Chinese history label should exist');
assertContains(/launch_history_summary:\s*'View all historical launches'/, 'English history label should exist');
assertContains(/past_launch_toggle_label:\s*'显示历史发射（默认关闭）'/, 'Chinese historical layer label should exist');
assertContains(/past_launch_toggle_label:\s*'Show historical launches \(off by default\)'/, 'English historical layer label should exist');
assertContains(/historyItems\.map\(item\s*=>/, 'all historical rows should be rendered into the list');
assertContains(/Math\.min\(\.\.\.pastDates\)/, 'history summary should include the oldest date');
assertContains(/Math\.max\(\.\.\.pastDates\)/, 'history summary should include the newest date');
assertContains(/let pastLaunchLayerGroup = L\.layerGroup\(\)/, 'past launches should use a separate Leaflet layer');
assertContains(/marker\.addTo\(pastLaunchLayerGroup\)/, 'past markers should be added only to the separate layer');
assertContains(/launchToggle\.checked && pastLaunchToggle\.checked/, 'past layer should require both launch toggles');
assertContains(/if \(pastLaunchToggle\.checked\) \{[\s\S]*pastLaunches\.forEach/, 'Cesium should render past launches only when enabled');
assertContains(/pastLaunchToggle\.checked = false/, 'historical launch layer should be forced off during initialization');

console.log('test_index_launch_history_list.js passed');

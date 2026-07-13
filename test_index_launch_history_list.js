#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, msg) {
  if (!re.test(html)) throw new Error(msg);
}

assertContains(/id="launchHistorySummary"/, 'launch history summary control should exist');
assertContains(/id="launchHistoryList"/, 'launch history list should exist');
assertContains(/launch_history_summary:\s*'查看全部历史发射记录'/, 'Chinese history label should exist');
assertContains(/launch_history_summary:\s*'View all historical launches'/, 'English history label should exist');
assertContains(/historyItems\.map\(item\s*=>/, 'all historical rows should be rendered into the list');
assertContains(/Math\.min\(\.\.\.pastDates\)/, 'history summary should include the oldest date');
assertContains(/Math\.max\(\.\.\.pastDates\)/, 'history summary should include the newest date');

console.log('test_index_launch_history_list.js passed');

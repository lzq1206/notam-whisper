#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, msg) {
  if (!re.test(html)) throw new Error(msg);
}

assertContains(/const\s+deviceLanguage\s*=/, 'deviceLanguage variable should exist');
assertContains(/navigator\.languages/, 'device language detection should check navigator.languages');
assertContains(/navigator\.language/, 'device language detection should check navigator.language');
assertContains(/const\s+locale\s*=/, 'locale variable should exist');
assertContains(/\/\^zh\\b\/\.test\(deviceLanguage\)/, 'locale detection should test zh prefix');
assertContains(/:\s*'en'/, 'locale should default to English for non-Chinese languages');
assertContains(/const\s+I18N\s*=\s*\{[\s\S]*\bzh:\s*\{[\s\S]*\ben:\s*\{[\s\S]*\}/, 'I18N dictionary should include zh and en locales');
assertContains(/document\.documentElement\.lang\s*=\s*locale\s*===\s*'zh'\s*\?\s*'zh-CN'\s*:\s*'en'/, 'html lang should be set from selected locale');
assertContains(/document\.title\s*=\s*t\('page_title'\)/, 'page title should be localized');
assertContains(/function\s+applyLocaleTexts\s*\(/, 'applyLocaleTexts function should exist');
assertContains(/applyLocaleTexts\(\)/, 'applyLocaleTexts should be invoked');

console.log('test_index_i18n_locale.js passed');

#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

function assertContains(re, msg) {
  if (!re.test(html)) throw new Error(msg);
}

assertContains(/id="downloadKmlBtn"/, 'download KML button should exist in HTML');
assertContains(/function\s+generateCurrentViewKml\s*\(/, 'generateCurrentViewKml function should exist');
assertContains(/function\s+downloadCurrentKml\s*\(/, 'downloadCurrentKml function should exist');
assertContains(/downloadKmlBtn\.addEventListener\(\s*'click'\s*,\s*downloadCurrentKml\s*\)/, 'download button click handler should be wired');

assertContains(/kml_type_notam/, 'KML description should use localized NOTAM type text key');
assertContains(/kml_type_msi/, 'KML description should use localized MSI type text key');
assertContains(/kml_type_launch/, 'KML description should use localized launch type text key');
assertContains(/kml_raw/, 'KML should include localized raw text label key');
assertContains(/kml_details/, 'KML should include localized launch details label key');
assertContains(/<LineStyle><color>ff2b2bef<\/color><width>2<\/width><\/LineStyle>/, 'NOTAM style should include outline line style');
assertContains(/<PolyStyle><color>552b2bef<\/color><\/PolyStyle>/, 'NOTAM style should include polygon fill style');
assertContains(/<LineStyle><color>ff0099ff<\/color><width>2<\/width><\/LineStyle>/, 'MSI style should include outline line style');
assertContains(/<PolyStyle><color>550099ff<\/color><\/PolyStyle>/, 'MSI style should include polygon fill style');

console.log('test_index_kml_download.js passed');

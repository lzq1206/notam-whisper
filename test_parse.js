const fs = require('fs');

// Simple CSV parser for quoted fields
function parseCSV(text) {
    let lines = [];
    let curLine = [];
    let curField = '';
    let inQuotes = false;
    for (let i = 0; i < text.length; i++) {
        let char = text[i];
        if (inQuotes) {
            if (char === '"') {
                if (i+1 < text.length && text[i+1] === '"') {
                    curField += '"';
                    i++;
                } else {
                    inQuotes = false;
                }
            } else {
                curField += char;
            }
        } else {
            if (char === '"') {
                inQuotes = true;
            } else if (char === ',') {
                curLine.push(curField);
                curField = '';
            } else if (char === '\n') {
                if (curField.endsWith('\r')) curField = curField.slice(0, -1);
                curLine.push(curField);
                lines.push(curLine);
                curLine = [];
                curField = '';
            } else {
                curField += char;
            }
        }
    }
    if (curField || text[text.length-1]===',') curLine.push(curField);
    if (curLine.length > 0) lines.push(curLine);
    
    let headers = lines[0];
    let rows = [];
    for(let i=1; i<lines.length; i++) {
        let obj = {};
        for(let j=0; j<headers.length; j++) {
            obj[headers[j]] = lines[i][j];
        }
        rows.push(obj);
    }
    return rows;
}

try {
    const csv = fs.readFileSync('downloaded_latest.csv', 'utf-8');
    const rows = parseCSV(csv);
    console.log('Parsed rows:', rows.length);
    
    const spacex = rows.find(r => r.raw && r.raw.includes('SPACEX'));
    if(spacex) {
        console.log('SPACEX NOTAM:', spacex.notam_id);
        console.log('RAW length:', spacex.raw.length);
        
        // Exact logic from index.html
        let raw = spacex.raw;
        let cleanedRaw = raw.replace(/Q\).*?(?=\s*A\))/s, '');
        console.log('Cleaned length:', cleanedRaw.length);
        
        let groups = {};
        cleanedRaw = cleanedRaw
            .replace(/\b(\d{2,3})\s+(\d{2}(?:[.,]\d+)?)(?:\s+(\d{2}(?:[.,]\d+)?))?\s*([NSEW])\b/gi, (match, d, m, s, hemi) => {
                return d + m + (s || '') + hemi.toUpperCase();
            })
            .replace(/(^|[\s,-])([NSEW])\s*(\d{2,3})\s+(\d{2}(?:[.,]\d+)?)(?:\s+(\d{2}(?:[.,]\d+)?))?\b/gi, (match, pref, hemi, d, m, s) => {
                return pref + d + m + (s || '') + hemi.toUpperCase();
            });
            
        const origMatchesMatches = [...raw.matchAll(/(?:([NS])\s*(\d{4,6}(?:[.,]\d+)?))\s*(?:([EW])\s*(\d{5,7}(?:[.,]\d+)?))|(?:(\d{4,6}(?:[.,]\d+)?)\s*([NS]))\s*(?:(\d{5,7}(?:[.,]\d+)?)\s*([EW]))/gi)];
        
        const regex = /(?:([NS])\s*(\d{4,6}(?:[.,]\d+)?))\s*(?:([EW])\s*(\d{5,7}(?:[.,]\d+)?))|(?:(\d{4,6}(?:[.,]\d+)?)\s*([NS]))\s*(?:(\d{5,7}(?:[.,]\d+)?)\s*([EW]))/gi;
        const matches = [...cleanedRaw.matchAll(regex)];
        
        console.log('REGEX JS MATCHES FOUND:', matches.length);
        console.log('ORIG MATCHES FOUND:', origMatchesMatches.length);
        
        if (matches.length > 0) {
            console.log('First matched text:', matches[0][0]);
        }
    } else {
        console.log('SPACEX NOT FOUND');
    }
} catch(e) { console.error(e) }

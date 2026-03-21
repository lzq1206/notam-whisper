const raw = "Q0678/26 NOTAMN Q) RJJJ/QARCH/IV/BO/E/000/999/3310N14118E999 A) RJJJ B) 2603221441 C) 2603221616 E) ALTN RTE ARE ESTABLISHED DUE TO AN AEROSPACE FLIGHT ACTIVITY. 1.FLT PLANNED RTE IS REQ TO BE FILED AS FLW   BUICK - Q1 - AKUSI:    ONC - V73 - HIDEK   OR SAKON - R595 - TUNTO   BUICK - V75 - CANAI:    ONC - V73 - HIDEK   OR SAKON - R595 - TUNTO   AZAMA - Y74 - TOPAT:    ONC - V73 - HIDEK   OR SAKON - R595 - TUNTO   TEKOS - Y78 - AVLAS:    ONC - V73 - HIDEK   OR SAKON - R595 - TUNTO   MDE - A590 - TUNTO:    (FOR RNP10) BIXAK - 2400N13140E - GURAG    (FOR NON RNP10) BIXAK - 2330N13200E - GURAG   AVLAS - R584 - SALVA:    TUNTO - R595 - SALVA  2.SEE NOTAM RJAAYNYX P1361/26, P1362/26 F) SFC G) UNL  CREATED: 19 MAR 2026 10:38 SOURCE: RJAAYNYX";

    function parseCoordinate(s, isLon) {
      let parts = s.split('.');
      let intPart = parts[0];
      let decPart = parts.length > 1 ? '.' + parts[1] : '';
      
      let d, m, sec = '0';
      if (isLon) {
        if (intPart.length === 4 || intPart.length === 5) {
          m = intPart.slice(-2) + decPart;
          d = intPart.slice(0, -2);
        } else if (intPart.length === 6 || intPart.length === 7) {
          sec = intPart.slice(-2) + decPart;
          m = intPart.slice(-4, -2);
          d = intPart.slice(0, -4);
        } else return null;
      } else {
        if (intPart.length === 3 || intPart.length === 4) {
          m = intPart.slice(-2) + decPart;
          d = intPart.slice(0, -2);
        } else if (intPart.length === 5 || intPart.length === 6) {
          sec = intPart.slice(-2) + decPart;
          m = intPart.slice(-4, -2);
          d = intPart.slice(0, -4);
        } else return null;
      }
      if (d === '') d = '0';
      return parseFloat(d) + parseFloat(m)/60 + parseFloat(sec)/3600;
    }

    function extractPolygonFromRaw(raw) {
      if (!raw) return [];
      // Remove Q-line to avoid mistakenly parsing the center coordinate
      const cleanedRaw = raw.replace(/Q\).*?(?=\s*A\))/s, '');

      const regex = /(?:([NS])\s*(\d{4,6}(?:[.,]\d+)?))\s*(?:([EW])\s*(\d{5,7}(?:[.,]\d+)?))|(?:(\d{4,6}(?:[.,]\d+)?)\s*([NS]))\s*(?:(\d{5,7}(?:[.,]\d+)?)\s*([EW]))/gi;
      const matches = [...cleanedRaw.matchAll(regex)];
      if (matches.length < 2) return [];
      
      const groups = {};
      
      for (const m of matches) {
        let isHemiFirst = !!m[1];
        let latHemi = (m[1] || m[6]).toUpperCase();
        let latStr  = (m[2] || m[5]).replace(',', '.');
        let lonHemi = (m[3] || m[8]).toUpperCase();
        let lonStr  = (m[4] || m[7]).replace(',', '.');
        
        let latIntLen = latStr.split('.')[0].length;
        let lonIntLen = lonStr.split('.')[0].length;
        
        // Q-line center point is always DDMM/DDDMM (<=4 / <=5). Polygons are often DDMMSS/DDDMMSS (>4 / >5).
        let latType = (latIntLen > 4) ? 'SEC' : 'MIN';
        let lonType = (lonIntLen > 5) ? 'SEC' : 'MIN';
        let matchType = (isHemiFirst ? 'PRE' : 'SUF') + '_' + latType + '_' + lonType;
        
        let latObj = parseCoordinate(latStr, false);
        let lonObj = parseCoordinate(lonStr, true);
        if (latObj === null || lonObj === null) continue;
        
        let lat = latObj;
        if (latHemi === 'S') lat = -lat;
        
        let lon = lonObj;
        if (lonHemi === 'W') lon = -lon;
        
        if (!groups[matchType]) groups[matchType] = [];
        groups[matchType].push({ lat: lat, lon: lon, index: m.index });
      }
      
      let allValidSubGroups = [];
       console.log("Groups:", JSON.stringify(groups, null, 2));

      for (const t in groups) {
        let currentSubGroup = [];
        let lastIndex = -1;
        
        for (const pt of groups[t]) {
          // If distance between start of coordinates is too large (> 150 chars), they are likely unrelated scattered points
          if (lastIndex === -1 || (pt.index - lastIndex) < 150) {
            currentSubGroup.push([pt.lat, pt.lon]);
          } else {
            if (currentSubGroup.length >= 2) {
              allValidSubGroups.push(currentSubGroup);
            }
            currentSubGroup = [[pt.lat, pt.lon]];
          }
          lastIndex = pt.index;
        }
        
        if (currentSubGroup.length >= 2) {
          allValidSubGroups.push(currentSubGroup);
        }
      }
      
      return allValidSubGroups;
    }

console.dir(extractPolygonFromRaw(raw), {depth: null});

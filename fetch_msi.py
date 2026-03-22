#!/usr/bin/env python3
"""
NGA MSI Maritime Warning fetcher.
Outputs: msi.csv, msi_raw.csv, msi.kml, history/msi/YYYY-WNN.*
"""
import requests, re, os, datetime, csv, math, time, urllib3
import xml.etree.ElementTree as ET
import urllib.request
import ssl

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
now_utc = datetime.datetime.utcnow()

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw','polygon']
RAW_CSV_HEADERS = ['notam_id','category','from_utc','to_utc','raw']

MONTHS_MAP = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
              'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

AEROSPACE_KEYWORDS = ['ROCKET', 'LAUNCH', 'SPACE', 'DEBRIS', 'SATELLITE', 'RE-ENTRY', 'REENTRY', 'AEROSPACE', 'SPLASH']

# ═══════════════════════════════════════════════════════════════
# MSI Coordinate & Time Parsing
# ═══════════════════════════════════════════════════════════════
def msi_coord_to_dd(deg, mm, dec, hemi):
    sec = round(int(dec) * 60 / 100)
    val = int(deg) + int(mm)/60.0 + sec/3600.0
    if hemi in ('S','W'):
        val = -val
    return val

def parse_msi_cancel_time(text):
    m = re.search(r'CANCEL\s+THIS\s+MSG\s+(\d{2})(\d{4})Z\s+([A-Z]+)\s+(\d{2})', text, re.I)
    if m:
        day, hhmm, mon, yr = m.groups()
        try:
            h = int(hhmm[:2]) if len(hhmm)>=2 else 0
            m_val = int(hhmm[2:4]) if len(hhmm)>=4 else 0
            return datetime.datetime(2000+int(yr), MONTHS_MAP.get(mon.upper(),1), int(day), h, m_val)
        except:
            pass
    return None

def parse_msi_coords_multi(text):
    """
    Returns a list of coordinate lists, one for each area (e.g. A., B...).
    If no A/B markers found, returns one list in the outer list.
    """
    if 'BOUND BY' not in text.upper():
        return []
        
    parts = re.split(r'BOUND BY[\s:]*', text, flags=re.I)
    if len(parts) < 2:
        return []
    
    content = parts[1]
    # Split further by A. B. C. if present
    sub_parts = re.split(r'\s+([A-Z]\.)\s+', content)
    
    areas = []
    current_area_text = ""
    
    if len(sub_parts) == 1:
        current_area_text = sub_parts[0]
        c = find_coords_in_text(current_area_text)
        if c: areas.append(c)
    else:
        # sub_parts will be like ["first part before A.", "A.", "text after A.", "B.", "text after B."]
        # Or even just ["text before A. (empty usually)", "A.", "text after A.", ...]
        # We look for groups
        current_text = sub_parts[0]
        for i in range(1, len(sub_parts), 2):
            if i+1 < len(sub_parts):
                label = sub_parts[i]
                text_block = sub_parts[i+1]
                c = find_coords_in_text(text_block)
                if c: areas.append(c)
    
    if not areas:
        # Final fallback: search everything
        c = find_coords_in_text(text)
        if c: areas.append(c)
        
    return areas

def find_coords_in_text(text):
    coords = []
    # 1. Standard NGA format: DD-MM.mmN DDD-MM.mmW
    pattern1 = r'(\d{1,2})[- \.]*(\d{2})(?:[ \.]*(\d+))?\s*([NS])\s*(\d{2,3})[- \.]*(\d{2})(?:[ \.]*(\d+))?\s*([EW])'
    for m in re.finditer(pattern1, text):
        deg1, min1, dec1, hemi1, deg2, min2, dec2, hemi2 = m.groups()
        dec1 = dec1 if dec1 else '0'
        dec2 = dec2 if dec2 else '0'
        coords.append((msi_coord_to_dd(deg1, min1, dec1, hemi1), msi_coord_to_dd(deg2, min2, dec2, hemi2)))

    if not coords:
        # 2. Loose format: DDMMN DDDMMW
        pattern2 = r'(\d{2})(\d{2})\s*([NS])\s*(\d{3})(\d{2})\s*([EW])'
        for m in re.finditer(pattern2, text):
            d1, m1, h1, d2, m2, h2 = m.groups()
            coords.append((msi_coord_to_dd(d1, m1, '0', h1), msi_coord_to_dd(d2, m2, '0', h2)))
    return coords

# ═══════════════════════════════════════════════════════════════
# MSI Fetching (navwarns-style)
# ═══════════════════════════════════════════════════════════════
def make_msi_headers():
    return {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Accept": "application/xml, text/xml, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://msi.nga.mil/",
        "Connection": "keep-alive"
    }

LOG_FILE = 'msi_fetch_log.txt'
HTML_CHECK_PREFIX_LENGTH = 200
PRIMARY_MSI_URL_TEMPLATE = "https://msi.nga.mil/api/publications/smaps?navArea={nav_area}&status=active&output=xml"
FALLBACK_MSI_URL_TEMPLATE = os.getenv("MSI_FALLBACK_URL_TEMPLATE", "").strip()

DAILY_MEMO_URLS = {
    '4': 'https://msi.nga.mil/apology_objects/DailyMemIV.txt',
    '12': 'https://msi.nga.mil/apology_objects/DailyMemXII.txt',
    'A': 'https://msi.nga.mil/apology_objects/DailyMemLAN.txt',
    'P': 'https://msi.nga.mil/apology_objects/DailyMemPAC.txt',
    'C': 'https://msi.nga.mil/apology_objects/DailyMemARC.txt'
}

def parse_msi_text_memo(text):
    text = text.replace('\r\n', '\n')
    blocks = re.split(r'\n(?=\d{6}Z [A-Z]{3} \d{2}\n)', text)
    res = []
    for block in blocks:
        block = block.strip()
        if not block: continue
        if not re.match(r'\d{6}Z [A-Z]{3} \d{2}', block):
            continue
        lines = block.split('\n')
        if len(lines) < 2: continue
        msg_id_line = lines[1].strip()
        msg_id_match = re.search(r'^([A-Z]+(?:\s+[A-Z\d]+)?\s+\d+/\d+)', msg_id_line)
        msg_id = msg_id_match.group(1) if msg_id_match else msg_id_line
        res.append({
            'msgID': msg_id,
            'msgText': block,
            'category': 'MARITIME',
            'msgType': 'Warning'
        })
    return res

def fetch_msi_from_txt(nav_area):
    url = DAILY_MEMO_URLS.get(nav_area)
    if not url: return []
    log_to_file(f"[FETCH] (text-memo) Area {nav_area} URL: {url}")
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, headers=make_msi_headers())
        with urllib.request.urlopen(req, context=ctx, timeout=60) as response:
            status = response.getcode()
            text = response.read().decode('utf-8', errors='replace').strip()
            if status == 200:
                return parse_msi_text_memo(text)
    except Exception as e:
        log_to_file(f'  Text memo fetch error: {e}')
    return []

def log_to_file(msg):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.datetime.utcnow().isoformat()}] {msg}\n")
    print(msg)

# Initialize log file
with open(LOG_FILE, 'w', encoding='utf-8') as f:
    f.write("MSI Fetch Log\n")

def fetch_msi_single(nav_area, url_template=PRIMARY_MSI_URL_TEMPLATE, source_name='primary'):
    url = url_template.format(nav_area=nav_area)
    log_to_file(f"[FETCH] ({source_name}) Area {nav_area} URL: {url}")
    MAX_RETRIES = 4
    RETRY_BACKOFF = 2.0
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers=make_msi_headers())
            with urllib.request.urlopen(req, context=ctx, timeout=60) as response:
                status = response.getcode()
                text = response.read().decode('utf-8', errors='replace').strip()
                if status == 200 and text.startswith('<') and "<html" not in text[:HTML_CHECK_PREFIX_LENGTH].lower():
                    if '<smapsActiveEntity' in text:
                        root = ET.fromstring(text)
                        entities = root.findall('smapsActiveEntity')
                        res = []
                        for entity in entities:
                            res.append({
                                'msgID': entity.findtext('msgID'),
                                'msgText': entity.findtext('msgText'),
                                'category': entity.findtext('category'),
                                'msgType': entity.findtext('msgType')
                            })
                        return res
        except Exception as e:
            log_to_file(f'  Request error: {e}')
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF ** attempt)
    return []

def fetch_msi_single_with_fallback(nav_area):
    res = fetch_msi_single(nav_area, PRIMARY_MSI_URL_TEMPLATE, 'primary')
    if res: return res
    if FALLBACK_MSI_URL_TEMPLATE:
        res = fetch_msi_single(nav_area, FALLBACK_MSI_URL_TEMPLATE, 'fallback')
        if res: return res
    if nav_area in DAILY_MEMO_URLS:
        return fetch_msi_from_txt(nav_area)
    return []

def fetch_msi():
    nav_areas = ['4', '12', 'A', 'P', 'C', '1', '2', '3', '5', '6', '7', '8', '9', '10', '11']
    all_smaps = []
    for na in nav_areas:
        res = fetch_msi_single_with_fallback(na)
        all_smaps.extend(res)
        time.sleep(2)
    return process_msi_data(all_smaps)

def parse_msi_active_times(text):
    # 1. Standard format: 080230Z TO 080430Z MAR 26
    m1 = re.search(r'(\d{2})(\d{4})Z\s*(?:[A-Z]{3}\s*)?TO\s*(\d{2})(\d{4})Z\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text, re.I)
    if m1:
        d1, t1, d2, t2, mon, yr = m1.groups()
        if yr is None: yr = datetime.datetime.utcnow().year
        else:
            yr = int(yr)
            if yr < 100: yr += 2000
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                h2 = int(t2[:2]) if len(t2)>=2 else 0
                m2_val = int(t2[2:4]) if len(t2)>=4 else 0
                to_dt = datetime.datetime(yr, month, int(d2), h2, m2_val)
                m1_val, y1 = month, yr
                if int(d1) > int(d2): # Likely spans across months
                    m1_val -= 1
                    if m1_val < 1: m1_val = 12; y1 -= 1
                h1 = int(t1[:2]) if len(t1)>=2 else 0
                m1_t = int(t1[2:4]) if len(t1)>=4 else 0
                from_dt = datetime.datetime(y1, m1_val, int(d1), h1, m1_t)
                return from_dt, to_dt
            except: pass

    # 2. THRU format: 01 THRU 31 MAR [YYYY]
    m2 = re.search(r'(\d{2})\s+THRU\s+(\d{2})\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text, re.I)
    if m2:
        d1, d2, mon, yr = m2.groups()
        if yr is None: yr = datetime.datetime.utcnow().year
        else:
            yr = int(yr); 
            if yr < 100: yr += 2000
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                from_dt = datetime.datetime(yr, month, int(d1), 0, 0)
                to_dt = datetime.datetime(yr, month, int(d2), 23, 59)
                if int(d1) > int(d2): # Cross-month? Usually "01 MAR THRU 02 APR" but NGA is weird.
                    # If d1 > d2, assume d1 is previous month
                    m1_val = month - 1
                    y1 = yr
                    if m1_val < 1: m1_val = 12; y1 -= 1
                    from_dt = datetime.datetime(y1, m1_val, int(d1), 0, 0)
                return from_dt, to_dt
            except: pass

    # 3. UNTIL format: UNTIL 181600Z MAR [YYYY]
    m3 = re.search(r'UNTIL\s+(\d{2})(\d{4})Z\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text, re.I)
    if m3:
        d2, t2, mon, yr = m3.groups()
        if yr is None: yr = datetime.datetime.utcnow().year
        else:
            yr = int(yr); 
            if yr < 100: yr += 2000
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                h2 = int(t2[:2])
                m2_val = int(t2[2:4])
                to_dt = datetime.datetime(yr, month, int(d2), h2, m2_val)
                # For "UNTIL", start is usually "now" or relative to creation
                from_dt = datetime.datetime.utcnow()
                return from_dt, to_dt
            except: pass

    # 4. Fallback: creation date for from_dt if known, otherwise None
    return None, None

def process_msi_data(all_smaps):
    rows = []
    seen = set()
    for s in all_smaps:
        msg_id = s.get('msgID', '')
        if not msg_id: msg_id = str(hash(s.get('msgText','')))[:10]
        if msg_id in seen: continue
        seen.add(msg_id)
        msg_text = s.get('msgText', '')
        if not msg_text: continue
        
        # Aerospace Filter
        is_aerospace = any(k in msg_text.upper() for k in AEROSPACE_KEYWORDS)
        if not is_aerospace:
            continue

        multi_coords = parse_msi_coords_multi(msg_text)
        if not multi_coords: continue
        
        # Calculate center for backwards compatibility and single-point representation
        all_flattened = []
        for area in multi_coords: all_flattened.extend(area)
        clat = sum(c[0] for c in all_flattened)/len(all_flattened)
        clon = sum(c[1] for c in all_flattened)/len(all_flattened)

        from_dt, to_dt = parse_msi_active_times(msg_text)
        clean_text = msg_text.replace('\n', '  ').replace('\r', '').replace('"', "'")
        
        rows.append({
            'notam_id': msg_id,
            'raw': clean_text,
            'coords': multi_coords, # Now list of lists
            'lat': clat,
            'lon': clon,
            'source': 'MSI',
            'category': s.get('category', 'MARITIME'),
            'from_utc': from_dt.isoformat() + "Z" if from_dt else "",
            'to_utc': to_dt.isoformat() + "Z" if to_dt else ""
        })
    return rows

def write_msi_raw_csv(rows, csv_path='msi_raw.csv'):
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(RAW_CSV_HEADERS)
        for r in rows:
            writer.writerow([r.get('notam_id', ''), r.get('category', ''), r.get('from_utc', ''), r.get('to_utc', ''), r.get('raw', '')])

def csv_to_kml(csv_path, kml_path):
    rows = []
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
    kml = ET.Element('kml', xmlns='http://www.opengis.net/kml/2.2')
    doc = ET.SubElement(kml, 'Document')
    ET.SubElement(doc, 'name').text = 'Maritime Warnings'
    for pm_row in rows:
        lat, lon = pm_row.get('lat', ''), pm_row.get('lon', '')
        if not lat or not lon: continue
        pm = ET.SubElement(doc, 'Placemark')
        ET.SubElement(pm, 'name').text = pm_row.get('notam_id', 'MSI')
        ET.SubElement(pm, 'description').text = pm_row.get('raw', '')[:500]
        
        poly = pm_row.get('polygon', '')
        if poly and poly.startswith('['):
            try:
                import json
                coords_list = json.loads(poly)
                # coords_list is [[(lat,lon),...], [(lat,lon),...]] or similar
                if coords_list and isinstance(coords_list[0], list):
                    if len(coords_list) > 1:
                        mg = ET.SubElement(pm, 'MultiGeometry')
                        for ring in coords_list:
                            if not ring: continue
                            geom = ET.SubElement(mg, 'Polygon')
                            bi = ET.SubElement(geom, 'outerBoundaryIs')
                            lr = ET.SubElement(bi, 'LinearRing')
                            cs = [f"{pt[1]},{pt[0]},0" for pt in ring]
                            if cs[0] != cs[-1]: cs.append(cs[0])
                            ET.SubElement(lr, 'coordinates').text = " ".join(cs)
                    else:
                        multi = ET.SubElement(pm, 'Polygon')
                        bi = ET.SubElement(multi, 'outerBoundaryIs')
                        lr = ET.SubElement(bi, 'LinearRing')
                        cs = [f"{pt[1]},{pt[0]},0" for pt in coords_list[0]]
                        if cs[0] != cs[-1]: cs.append(cs[0])
                        ET.SubElement(lr, 'coordinates').text = " ".join(cs)
                else: raise ValueError()
            except:
                point = ET.SubElement(pm, 'Point')
                ET.SubElement(point, 'coordinates').text = f"{lon},{lat},0"
        else:
            point = ET.SubElement(pm, 'Point')
            ET.SubElement(point, 'coordinates').text = f"{lon},{lat},0"
    tree = ET.ElementTree(kml)
    ET.indent(tree, space='  ')
    tree.write(kml_path, xml_declaration=True, encoding='UTF-8')

def archive_weekly(csv_path, history_subdir='msi'):
    history_dir = os.path.join('history', history_subdir)
    os.makedirs(history_dir, exist_ok=True)
    today = datetime.date.today()
    iso_year, iso_week, _ = today.isocalendar()
    week_tag = f"{iso_year}-W{iso_week:02d}"
    weekly_csv = os.path.join(history_dir, f"{week_tag}.csv")
    new_rows = []
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            new_rows = list(reader)
    existing_rows = []
    if os.path.exists(weekly_csv):
        with open(weekly_csv, 'r', encoding='utf-8') as f:
            existing_rows = list(csv.DictReader(f))
    seen = set()
    merged = []
    for r in new_rows:
        nid = r.get('notam_id', '')
        if nid and nid not in seen:
            seen.add(nid)
            merged.append(r)
    for r in existing_rows:
        nid = r.get('notam_id', '')
        if nid and nid not in seen:
            seen.add(nid)
            merged.append(r)
    with open(weekly_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(merged)
    csv_to_kml(weekly_csv, weekly_csv.replace('.csv', '.kml'))

def main():
    msi_rows = fetch_msi()
    write_msi_raw_csv(msi_rows, 'msi_raw.csv')
    with open('msi.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        for r in msi_rows:
            # r['coords'] is now a list of lists [[(lat,lon),...], [...]]
            import json
            poly_json = json.dumps(r['coords'])
            writer.writerow(['Maritime', '', r['notam_id'], 'MSI', r.get('from_utc', ''), r.get('to_utc', ''), round(r['lat'], 6), round(r['lon'], 6), '', '', r['raw'], poly_json])
    csv_to_kml('msi.csv', 'msi.kml')
    archive_weekly('msi.csv', 'msi')

if __name__ == '__main__':
    try: main()
    except Exception as e:
        log_to_file(f"CRITICAL ERROR: {e}")
        import traceback
        log_to_file(traceback.format_exc())

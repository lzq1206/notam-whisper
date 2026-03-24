#!/usr/bin/env python3
"""
MSI (Maritime Safety Information) Fetcher
NGA API: msi.nga.mil
Outputs: msi.csv, msi.kml, msi_raw.csv
"""
import requests, re, os, datetime, csv, time, json
import xml.etree.ElementTree as ET
import urllib3
import ssl

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
PRIMARY_MSI_URL_TEMPLATE = "https://msi.nga.mil/api/publications/ntm/warnings?output=xml&navArea={nav_area}"
FALLBACK_MSI_URL_TEMPLATE = None 
MAX_RETRIES = 3
RETRY_BACKOFF = 2

# For "Daily Memo" textual warnings sometimes found on the site
DAILY_MEMO_URLS = {
    '4':  'https://msi.nga.mil/apology_objects/DailyMemLANT.txt',
    '12': 'https://msi.nga.mil/apology_objects/DailyMemPAC.txt',
}

MONTHS_MAP = {
    'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
    'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12
}

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw','polygon']
RAW_CSV_HEADERS = ['msgID','category','from_utc','to_utc','msgText']

AEROSPACE_KEYWORDS = ["ROCKET", "LAUNCH", "SPACE", "RE-ENTRY", "DEBRIS", "AEROSPACE", "SATELLITE", "MISSILE"]

def log_to_file(msg):
    with open('msi_fetch_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{datetime.datetime.now().isoformat()} - {msg}\n")

def _is_in_time_window(from_str, to_str):
    """Return True if the record is not expired and not too far in the future."""
    now_utc = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    five_days = now_utc + datetime.timedelta(days=5)
    try:
        if from_str:
            from_dt = datetime.datetime.fromisoformat(from_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if from_dt > five_days:
                return False
        if to_str:
            to_dt = datetime.datetime.fromisoformat(to_str.replace('Z', '+00:00')).replace(tzinfo=None)
            if to_dt < now_utc:
                return False
    except Exception:
        pass
    return True

# --- Parsing Helpers ---
def parse_msi_coords_multi(text):
    """
    Parses complex coordinates like:
    A. 35-09.00N 120-39.00W, 34-08.00N 119-36.00W...
    B. 31-09.00N 124-59.00W...
    Returns a list of coordinate rings (List[List[Tuple[float, float]]])
    """
    if not text: return []
    
    # Split by sections A., B., C. or just treat as one if none
    sections = re.split(r'\b[A-Z]\.\s', text)
    if not sections or (len(sections) == 1 and not re.search(r'\d{2}-\d{2}\.\d+[NS]', text)):
        # Try to find any coordinates if no lettered sections
        sections = [text]

    all_rings = []
    # Pattern: DD-MM.mmN DDD-MM.mmE
    coord_pattern = r'(\d{1,3})-(\d{2}\.\d+)\s*([NS])\s+(\d{1,3})-(\d{2}\.\d+)\s*([EW])'
    
    for sec in sections:
        ring = []
        for match in re.finditer(coord_pattern, sec):
            lat_d, lat_m, lat_h, lon_d, lon_m, lon_h = match.groups()
            lat = float(lat_d) + float(lat_m)/60.0
            if lat_h.upper() == 'S': lat = -lat
            lon = float(lon_d) + float(lon_m)/60.0
            if lon_h.upper() == 'W': lon = -lon
            ring.append((round(lat, 6), round(lon, 6)))
        if ring:
            all_rings.append(ring)
    return all_rings

def parse_msi_cancel_time(text):
    """
    Extracts cancellation date from 'CANCEL THIS MSG DDHHMMZ MON YY'
    """
    m = re.search(r'CANCEL\s+THIS\s+MSG\s+(\d{2})(\d{4})Z\s+([A-Z]{3})\s+(\d{2})', text, re.I)
    if m:
        day, hhmm, mon, yr = m.groups()
        try:
            return datetime.datetime(2000 + int(yr), MONTHS_MAP.get(mon.upper(), 1), int(day), int(hhmm[:2]), int(hhmm[2:4]))
        except: pass
    return None

def parse_msi_active_times(text):
    """
    Finds all potential dates in the text and returns (min_dt, max_dt).
    Handles multiple 'TO' windows, 'THRU', and 'CANCEL' messages.
    """
    found_dts = []
    
    # 0. Creation date at start of message: 240853Z FEB 26
    m0 = re.match(r'^(\d{2})(\d{4})Z\s+([A-Z]{3})\s+(\d{2})', text.strip())
    if m0:
        day, hhmm, mon, yr = m0.groups()
        try:
            y = 2000 + int(yr)
            m = MONTHS_MAP.get(mon.upper(), 1)
            found_dts.append(datetime.datetime(y, m, int(day), int(hhmm[:2]), int(hhmm[2:4])))
        except: pass

    # 1. Standard windows: DDHHMMZ TO DDHHMMZ MON [YYYY]
    # We use finditer to catch ALL windows (important for multi-stage launches)
    for m1 in re.finditer(r'(\d{2})(\d{4})Z\s*(?:[A-Z]{3}\s*)?TO\s*(\d{2})(\d{4})Z\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text, re.I):
        d1, t1, d2, t2, mon, yr = m1.groups()
        y_val = int(yr)+2000 if (yr and len(yr)==2) else (int(yr) if (yr and len(yr)==4) else datetime.datetime.utcnow().year)
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                h2, m2 = int(t2[:2]), int(t2[2:4])
                to_dt = datetime.datetime(y_val, month, int(d2), h2, m2)
                found_dts.append(to_dt)
                m1_val, y1 = month, y_val
                if int(d1) > int(d2): 
                    m1_val -= 1
                    if m1_val < 1: m1_val = 12; y1 -= 1
                from_dt = datetime.datetime(y1, m1_val, int(d1), int(t1[:2]), int(t1[2:4]))
                found_dts.append(from_dt)
            except: pass

    # 2. THRU format: DD THRU DD MON [YYYY]
    for m2 in re.finditer(r'(\d{1,2})\s+THRU\s+(\d{1,2})\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text, re.I):
        d1, d2, mon, yr = m2.groups()
        y_val = int(yr)+2000 if (yr and len(yr)==2) else (int(yr) if (yr and len(yr)==4) else datetime.datetime.utcnow().year)
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                found_dts.append(datetime.datetime(y_val, month, int(d2), 23, 59))
                m1_val, y1 = month, y_val
                if int(d1) > int(d2): 
                    m1_val -= 1
                    if m1_val < 1: m1_val = 12; y1 -= 1
                found_dts.append(datetime.datetime(y1, m1_val, int(d1), 0, 0))
            except: pass

    # 2b. Time-Only DAILY format: HHMMZ TO HHMMZ DAILY DD THRU DD MON [YYYY]
    for m2b in re.finditer(r'(\d{4})Z\s+TO\s+(\d{4})Z\s+DAILY\s+(\d{1,2})\s+THRU\s+(\d{1,2})\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text, re.I):
        t1, t2, d1, d2, mon, yr = m2b.groups()
        y_val = int(yr)+2000 if (yr and len(yr)==2) else (int(yr) if (yr and len(yr)==4) else datetime.datetime.utcnow().year)
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                found_dts.append(datetime.datetime(y_val, month, int(d2), int(t2[:2]), int(t2[2:4])))
                m1_val, y1 = month, y_val
                if int(d1) > int(d2): 
                    m1_val -= 1
                    if m1_val < 1: m1_val = 12; y1 -= 1
                found_dts.append(datetime.datetime(y1, m1_val, int(d1), int(t1[:2]), int(t1[2:4])))
            except: pass

    # 3. UNTIL format
    m3 = re.search(r'UNTIL\s+(\d{2})(\d{4})Z\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text, re.I)
    if m3:
        d2, t2, mon, yr = m3.groups()
        y_val = int(yr)+2000 if (yr and len(yr)==2) else (int(yr) if (yr and len(yr)==4) else datetime.datetime.utcnow().year)
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                found_dts.append(datetime.datetime(y_val, month, int(d2), int(t2[:2]), int(t2[2:4])))
            except: pass

    # 4. CANCEL date: CANCEL THIS MSG 010001Z APR 26
    c_dt = parse_msi_cancel_time(text)
    if c_dt: found_dts.append(c_dt)

    if not found_dts: return None, None
    return min(found_dts), max(found_dts)

# --- Fetching ---
def _find_warning_entities(root):
    entities = root.findall('.//warning')
    if entities:
        return entities
    entities = root.findall('.//smapsActiveEntity')
    if entities:
        return entities
    if root.tag in ('warning', 'smapsActiveEntity'):
        return [root]
    return []

def fetch_msi_single(nav_area, url_template, label):
    url = url_template.format(nav_area=nav_area)
    log_to_file(f"Fetching {label} NAVAREA {nav_area}...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30, verify=False)
            if resp.status_code == 200:
                root = ET.fromstring(resp.text)
                entities = _find_warning_entities(root)
                if entities:
                    res = []
                    for entity in entities:
                        res.append({
                            'msgID': entity.findtext('msgID') or '',
                            'msgText': entity.findtext('msgText') or '',
                            'category': entity.findtext('category'),
                            'msgType': entity.findtext('msgType')
                        })
                    return res
        except Exception as e:
            log_to_file(f"  Request error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF ** attempt)
    return []

def fetch_msi_from_txt(nav_area):
    url = DAILY_MEMO_URLS.get(nav_area)
    if not url: return []
    try:
        resp = requests.get(url, timeout=30, verify=False)
        if resp.status_code == 200:
            text = resp.text
            # Split by double newline or typical warning separator
            blocks = re.split(r'\n\s*\n', text)
            res = []
            for block in blocks:
                if len(block.strip()) > 50:
                    res.append({
                        'msgID': '', # Will be hashed
                        'msgText': block.strip(),
                        'category': 'Daily Memo',
                        'msgType': 'NavWarning'
                    })
            return res
    except: pass
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

def fetch_msi():
    nav_areas = ['4', '12', 'A', 'P', 'C', '1', '2', '3', '5', '6', '7', '8', '9', '10', '11']
    all_smaps = []
    for na in nav_areas:
        res = fetch_msi_single_with_fallback(na)
        all_smaps.extend(res)
        time.sleep(2)
    return process_msi_data(all_smaps)

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
                coords_list = json.loads(poly)
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
        if not _is_in_time_window(r.get('from_utc', ''), r.get('to_utc', '')):
            continue
        nid = r.get('notam_id', '')
        if nid and nid not in seen:
            seen.add(nid)
            merged.append(r)
    for r in existing_rows:
        if not _is_in_time_window(r.get('from_utc', ''), r.get('to_utc', '')):
            continue
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

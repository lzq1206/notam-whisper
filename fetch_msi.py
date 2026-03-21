#!/usr/bin/env python3
"""
NGA MSI Maritime Warning fetcher.
Outputs: msi.csv, msi_raw.csv, msi.kml, history/msi/YYYY-WNN.*
"""
import requests, re, os, datetime, csv, math, time, urllib3
import xml.etree.ElementTree as ET

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

now_utc = datetime.datetime.utcnow()

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw']
RAW_CSV_HEADERS = ['notam_id','category','from_utc','to_utc','raw']

MONTHS_MAP = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
              'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

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

def parse_msi_coords(text):
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
        pattern2 = r'(\d{2})(\d{2})\s*([NS])\s+(\d{3})(\d{2})\s*([EW])'
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
def log_to_file(msg):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.datetime.utcnow().isoformat()}] {msg}\n")
    print(msg)

# Initialize log file
with open(LOG_FILE, 'w', encoding='utf-8') as f:
    f.write("MSI Fetch Log\n")

def fetch_msi_single(nav_area):
    url = f"https://msi.nga.mil/api/publications/smaps?navArea={nav_area}&status=active&output=xml"
    log_to_file(f"[FETCH] Area {nav_area} URL: {url}")
    MAX_RETRIES = 4
    RETRY_BACKOFF = 2.0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=make_msi_headers(), timeout=60)
            resp.raise_for_status()
            text = resp.text.strip()
            log_to_file(f"  Attempt {attempt}: Status {resp.status_code}, Length {len(text)}")

            if text.startswith("<") and "<html" not in text[:200].lower():
                root = ET.fromstring(text)
                entities = root.findall('smapsActiveEntity')
                log_to_file(f"  Found {len(entities)} smapsActiveEntity nodes.")
                res = []
                for entity in entities:
                    res.append({
                        'msgID': entity.findtext('msgID'),
                        'msgText': entity.findtext('msgText'),
                        'category': entity.findtext('category'),
                        'msgType': entity.findtext('msgType')
                    })
                return res

            log_to_file(
                f"  Non-XML response (Content-Type: {resp.headers.get('content-type', 'unknown')})"
            )
            log_to_file(f"  Unexpected response snippet: {text[:200]}")
        except Exception as e:
            log_to_file(f"  Request error: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF ** attempt)
    return []

def fetch_msi():
    log_to_file("[MSI] Starting sequential fetch...")
    nav_areas = ['4', '12', 'A', 'P', 'C', '1', '2', '3', '5', '6', '7', '8', '9', '10', '11']
    all_smaps = []
    for na in nav_areas:
        res = fetch_msi_single(na)
        all_smaps.extend(res)
        time.sleep(2)
    
    log_to_file(f"[MSI] Total raw warnings: {len(all_smaps)}")
    return process_msi_data(all_smaps)

def parse_msi_active_times(text):
    # Pattern: 261122Z TO 261604Z MAR 26
    m = re.search(r'(\d{2})(\d{4})Z\s*(?:[A-Z]{3}\s*)?TO\s*(\d{2})(\d{4})Z\s+([A-Z]{3})(?:\s+(\d{2,4}))?', text)
    if m:
        d1, t1, d2, t2, mon, yr = m.groups()
        if yr is None: yr = datetime.datetime.utcnow().year
        else:
            yr = int(yr)
            if yr < 100: yr += 2000
        month = MONTHS_MAP.get(mon.upper())
        if month:
            try:
                h2 = int(t2[:2]) if len(t2)>=2 else 0
                m2 = int(t2[2:4]) if len(t2)>=4 else 0
                to_dt = datetime.datetime(yr, month, int(d2), h2, m2)
                m1, y1 = month, yr
                if int(d1) > int(d2):
                    m1 -= 1
                    if m1 < 1: m1 = 12; y1 -= 1
                h1 = int(t1[:2]) if len(t1)>=2 else 0
                m1_t = int(t1[2:4]) if len(t1)>=4 else 0
                from_dt = datetime.datetime(y1, m1, int(d1), h1, m1_t)
                return from_dt, to_dt
            except: pass
    return None, None

def process_msi_data(all_smaps):
    rows = []
    seen = set()
    five_days = now_utc + datetime.timedelta(days=5)

    for s in all_smaps:
        msg_id = s.get('msgID', '')
        if not msg_id: msg_id = str(hash(s.get('msgText','')))[:10]
        if msg_id in seen: continue
        seen.add(msg_id)
        msg_text = s.get('msgText', '')
        if not msg_text: continue

        coords = parse_msi_coords(msg_text)

        from_dt, to_dt = parse_msi_active_times(msg_text)

        clean_text = msg_text.replace('\n', '  ').replace('\r', '').replace('"', "'")
        from_utc_str = from_dt.isoformat() + "Z" if from_dt else ""
        to_utc_str = to_dt.isoformat() + "Z" if to_dt else ""

        rows.append({
            'notam_id': msg_id,
            'raw': clean_text,
            'coords': coords,
            'source': 'MSI',
            'category': s.get('category', 'MARITIME'),
            'from_utc': from_utc_str,
            'to_utc': to_utc_str
        })
    log_to_file(f"[MSI] Processed {len(rows)} rows.")
    return rows

def write_msi_raw_csv(rows, csv_path='msi_raw.csv'):
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(RAW_CSV_HEADERS)
        for r in rows:
            writer.writerow([
                r.get('notam_id', ''),
                r.get('category', ''),
                r.get('from_utc', ''),
                r.get('to_utc', ''),
                r.get('raw', '')
            ])

# ═══════════════════════════════════════════════════════════════
# KML Generation
# ═══════════════════════════════════════════════════════════════
def _parse_coord_val(digits, hemi):
    digits = digits.replace(',', '.')
    hemi = hemi.upper()
    is_lon = hemi in ('E', 'W')
    try:
        if '.' in digits:
            int_part, dec = digits.split('.')
        else:
            int_part, dec = digits, ''
        if is_lon:
            # Lon: DDDMM[SS]
            if len(int_part) <= 5:
                d = int(int_part[:3]) if len(int_part)>=3 else 0
                m = int(int_part[3:5]) if len(int_part)>=5 else (int(int_part[3:]) if len(int_part)>3 else 0)
                s = 0
            else:
                d = int(int_part[:3])
                m = int(int_part[3:5])
                s = int(int_part[5:7]) if len(int_part)>=7 else 0
        else:
            # Lat: DDMM[SS]
            if len(int_part) <= 4:
                d = int(int_part[:2]) if len(int_part)>=2 else 0
                m = int(int_part[2:4]) if len(int_part)>=4 else (int(int_part[2:]) if len(int_part)>2 else 0)
                s = 0
            else:
                d = int(int_part[:2])
                m = int(int_part[2:4])
                s = int(int_part[4:6]) if len(int_part)>=6 else 0
        val = d + m / 60.0 + s / 3600.0
        if dec:
            val += float(f"0.{dec}") / 60.0
        if hemi in ('S', 'W'):
            val = -val
        return round(val, 6)
    except:
        return 0.0

def csv_to_kml(csv_path, kml_path):
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    kml = ET.Element('kml', xmlns='http://www.opengis.net/kml/2.2')
    doc = ET.SubElement(kml, 'Document')
    ET.SubElement(doc, 'name').text = 'Maritime Warnings'

    for sid, color in [('polyStyle','7700aaff'), ('circleStyle','7700aaff')]:
        style = ET.SubElement(doc, 'Style', id=sid)
        ls = ET.SubElement(style, 'LineStyle')
        ET.SubElement(ls, 'color').text = 'ff0099ff'
        ET.SubElement(ls, 'width').text = '2'
        ps = ET.SubElement(style, 'PolyStyle')
        ET.SubElement(ps, 'color').text = color

    for row in rows:
        lat = row.get('lat', '').strip()
        lon = row.get('lon', '').strip()
        raw = row.get('raw', '')
        notam_id = row.get('notam_id', '')
        fir = row.get('fir', '')
        if not lat or not lon:
            continue
        try:
            lat_f, lon_f = float(lat), float(lon)
        except:
            continue

        pm = ET.SubElement(doc, 'Placemark')
        ET.SubElement(pm, 'name').text = notam_id or fir or 'MSI Warning'
        ET.SubElement(pm, 'description').text = raw[:500]

        ET.SubElement(pm, 'styleUrl').text = '#circleStyle'
        point = ET.SubElement(pm, 'Point')
        ET.SubElement(point, 'coordinates').text = f"{lon_f},{lat_f},0"

    tree = ET.ElementTree(kml)
    ET.indent(tree, space='  ')
    tree.write(kml_path, xml_declaration=True, encoding='UTF-8')
    print(f"[KML] Written {len(rows)} placemarks to {kml_path}")

# ═══════════════════════════════════════════════════════════════
# Weekly History Archiving
# ═══════════════════════════════════════════════════════════════
def archive_weekly(csv_path, history_subdir='msi'):
    history_dir = os.path.join('history', history_subdir)
    os.makedirs(history_dir, exist_ok=True)

    today = datetime.date.today()
    iso_year, iso_week, _ = today.isocalendar()
    week_tag = f"{iso_year}-W{iso_week:02d}"
    weekly_csv = os.path.join(history_dir, f"{week_tag}.csv")
    weekly_kml = os.path.join(history_dir, f"{week_tag}.kml")

    new_rows = []
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
        elif not nid:
            merged.append(r)
    for r in existing_rows:
        nid = r.get('notam_id', '')
        if nid and nid not in seen:
            seen.add(nid)
            merged.append(r)
        elif not nid:
            merged.append(r)

    with open(weekly_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(merged)

    csv_to_kml(weekly_csv, weekly_kml)
    print(f"[HISTORY] {week_tag}: {len(merged)} records ({len(new_rows)} new + {len(existing_rows)} existing)")

# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("NGA MSI Maritime Warning Fetcher")
    print("=" * 60)

    msi_rows = fetch_msi()

    write_msi_raw_csv(msi_rows, 'msi_raw.csv')

    # Write msi.csv
    with open('msi.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        for r in msi_rows:
            coords = r.get('coords', [])
            if not coords:
                continue
            clat = sum(float(c[0]) for c in coords) / len(coords)
            clon = sum(float(c[1]) for c in coords) / len(coords)
            writer.writerow([
                'Maritime', '', r['notam_id'], 'MSI',
                r.get('from_utc', ''), r.get('to_utc', ''),
                str(round(clat, 6)), str(round(clon, 6)), '', '',
                r['raw']
            ])

    print(f"\nPipeline complete: {len(msi_rows)} records written to msi_raw.csv; filtered geocoded records written to msi.csv")

    csv_to_kml('msi.csv', 'msi.kml')
    archive_weekly('msi.csv', 'msi')
    print("=" * 60)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log_to_file(f"CRITICAL ERROR in main: {e}")
        import traceback
        log_to_file(traceback.format_exc())

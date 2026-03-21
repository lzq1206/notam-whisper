#!/usr/bin/env python3
"""
NGA MSI Maritime Warning fetcher.
Outputs: msi.csv, msi.kml, history/msi/YYYY-WNN.*
"""
import requests, re, os, datetime, csv, math, time, urllib3
import xml.etree.ElementTree as ET

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

now_utc = datetime.datetime.utcnow()

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw']

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
            return datetime.datetime(2000+int(yr), MONTHS_MAP.get(mon.upper(),1), int(day), int(hhmm[:2]), int(hhmm[2:]))
        except:
            pass
    return None

def parse_msi_coords(text):
    coords = []
    for m in re.finditer(r'(\d{1,2})-(\d{2})\.(\d{2})([NS])\s+(\d{2,3})-(\d{2})\.(\d{2})([EW])', text):
        lat = msi_coord_to_dd(m.group(1), m.group(2), m.group(3), m.group(4))
        lon = msi_coord_to_dd(m.group(5), m.group(6), m.group(7), m.group(8))
        coords.append((lat, lon))
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

def fetch_msi_single(nav_area):
    url = f"https://msi.nga.mil/api/publications/smaps?navArea={nav_area}&status=active&category=14&output=xml"
    MAX_RETRIES = 4
    RETRY_BACKOFF = 2.0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=make_msi_headers(), timeout=60, verify=False)
            resp.raise_for_status()
            text = resp.text.strip()
            if text.startswith("<") and "<html" not in text[:200].lower():
                root = ET.fromstring(text)
                entries = []
                for entity in root.findall('smapsActiveEntity'):
                    entries.append({
                        'msgID': entity.findtext('msgID'),
                        'msgText': entity.findtext('msgText'),
                        'category': entity.findtext('category'),
                        'msgType': entity.findtext('msgType')
                    })
                return entries
            print(f"[MSI] Non-XML response for Area {nav_area} (Attempt {attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"[MSI] Request error for Area {nav_area} (Attempt {attempt}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF ** attempt)

    return []

def fetch_msi():
    print("[MSI] Starting sequential fetch for NAVAREAs...")
    nav_areas = ['4', '12', 'A', 'P', 'C', '1', '2', '3', '5', '6', '7', '8', '9', '10', '11']

    all_smaps = []
    for na in nav_areas:
        res = fetch_msi_single(na)
        if res:
            all_smaps.extend(res)
            print(f"      - Area {na}: {len(res)} messages fetched.")
        time.sleep(5)

    print(f"[MSI] Total raw warnings fetched: {len(all_smaps)}")
    return process_msi_data(all_smaps)

def process_msi_data(all_smaps):
    rows = []
    seen = set()
    for s in all_smaps:
        msg_id = s.get('msgID', '')
        if msg_id in seen:
            continue
        seen.add(msg_id)
        msg_text = s.get('msgText', '')
        if not msg_text:
            continue

        clean_text = msg_text.replace('\n', '  ').replace('\r', '').replace('"', "'")

        cancel = parse_msi_cancel_time(msg_text)
        if cancel and cancel < now_utc:
            continue

        coords = parse_msi_coords(msg_text)
        if len(coords) < 2:
            continue

        msg_type = s.get('msgType', '')
        code = msg_id if (msg_id and '/' in msg_id) else msg_type

        rows.append({
            'notam_id': code,
            'raw': clean_text,
            'coords': coords,
            'source': 'MSI',
            'category': s.get('category', 'MARITIME')
        })

    print(f"[MSI] Found {len(rows)} valid maritime warnings after filtering/parsing.")
    return rows

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
            if len(int_part) <= 5:
                d, m = int(int_part[:3]), int(int_part[3:5]) if len(int_part) >= 5 else int(int_part[3:])
                s = 0
            else:
                d, m, s = int(int_part[:3]), int(int_part[3:5]), int(int_part[5:7])
        else:
            if len(int_part) <= 4:
                d, m = int(int_part[:2]), int(int_part[2:4]) if len(int_part) >= 4 else int(int_part[2:])
                s = 0
            else:
                d, m, s = int(int_part[:2]), int(int_part[2:4]), int(int_part[4:6])
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
                'Maritime', '', r['notam_id'], 'MSI', '', '',
                str(round(clat, 6)), str(round(clon, 6)), '', '',
                r['raw']
            ])

    print(f"\nPipeline complete: {len(msi_rows)} records written to msi.csv")

    csv_to_kml('msi.csv', 'msi.kml')
    archive_weekly('msi.csv', 'msi')
    print("=" * 60)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Aerospace NOTAM fetcher — notammap.org only.
Outputs: notams.csv, notams.kml, history/notams/YYYY-WNN.*
"""
import requests, re, os, datetime, csv, math
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

# ─── Keyword Filters ───
KEEP = ["UNL", "AEROSPACE", "RE-ENTRY", "ROCKET"]
DROP = [
    "KWAJALEIN","BALLOON","BALLON","TRANSMITTER","GUNFIRING","AERIAL","GUN FRNG",
    "AIR EXER","REF AIP","MISSILES","KOLKATA","MWARA","ZS(D)","ZY(R)","ZG(R)",
    "SHIQUANHE","MEDEVAC","WOOMERA AIRSPACE","MAVLA","VED-52 ACT",
    "LASER DANGER AREA","ACFT MANEUVERING","ATTENTION ACFT","STNR ALT RESERVATION",
    "UNTIL PERM","MILITARY FLIGHTS","DUE MIL FLYING","EMERALD","SATPHONE",
    "OAKLAND ATC","UNLIT","UNLESS","ADS-B","UNLOAD","3000FT","UNMANNED ACFT","VVIP MOV",
    "FL200","400FT AGL","49215FT AMSL","1350FT AMSL","9000FT AMSL",
    "QUEENSLAND","LASER DISPLAY","UNLIGHTED","6-87 ROCKET","6-86 ROCKET","6-89 ROCKET",
    "RADIOSONDE","MODEL ROCKET","VOLCAN",
    "GPS INTERFERENCE","GPS JAMMING","NAVIGATION WARNING",
    "AIRSPAICE", "FIR RECIFE", "FIR SECT",
]

now_utc = datetime.datetime.utcnow()
five_days = now_utc + timedelta(days=5)

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw']
FAA_SEARCH_URL = "https://notams.aim.faa.gov/notamSearch/search"
FAA_SUPPLEMENTAL_FIRS = ["ZLHW", "ZHWH", "ZXXX"]
FAA_PAGE_SIZE = 30

def make_headers():
    return {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

def _passes_filters(notam):
    raw = notam.get('raw', '')
    raw_upper = raw.upper()
    if any(d in raw_upper for d in DROP):
        return False
    if not any(k in raw_upper for k in KEEP):
        return False
    from_str = notam.get('from', '')
    to_str = notam.get('to', '')
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

def _parse_faa_time(date_str):
    if not date_str:
        return ''
    if date_str == 'PERM':
        return ''
    try:
        dt = datetime.datetime.strptime(date_str, "%m/%d/%Y %H%M")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ''

def _parse_q_line(raw):
    qcode_match = re.search(r'Q\)\s*[A-Z0-9]{4}/([A-Z0-9]{5})/', raw)
    qcode = qcode_match.group(1) if qcode_match else ''

    center_match = re.search(
        r'Q\)\s*[A-Z0-9]{4}/[A-Z0-9]{5}/[^/]+/[^/]+/[^/]+/\d{3}/\d{3}/(\d{4,6})([NS])(\d{5,7})([EW])(\d{3})',
        raw,
        re.I
    )
    if not center_match:
        return '', '', '', qcode

    lat = _parse_coord_val(center_match.group(1), center_match.group(2))
    lon = _parse_coord_val(center_match.group(3), center_match.group(4))
    radius = center_match.group(5)
    if radius == '999':
        radius = ''
    return lat, lon, radius, qcode

def _normalize_notam_number(number):
    if not number:
        return '', '', ''
    # FAA `notamNumber` can be like A0787/26 or A0787/2026.
    m = re.match(r'^([A-Z])\s*0*(\d+)\s*/\s*(\d{2,4})$', number.strip(), re.I)
    if not m:
        return '', '', ''
    series, num, year = m.group(1).upper(), int(m.group(2)), m.group(3)
    if len(year) == 2:
        year = f"20{year}"
    return series, num, year

def fetch_faa_notams():
    rows = []
    headers = {
        **make_headers(),
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://notams.aim.faa.gov",
        "Referer": "https://notams.aim.faa.gov/notamSearch/nsapp.html",
    }

    for fir in FAA_SUPPLEMENTAL_FIRS:
        session = requests.Session()
        session.headers.update(headers)
        offset = 0
        while True:
            payload = {
                "searchType": "0",
                "designatorsForLocation": fir,
                "offset": str(offset),
                "notamsOnly": "false"
            }
            try:
                resp = session.post(FAA_SEARCH_URL, data=payload, timeout=30)
                if resp.status_code != 200:
                    break
                notam_list = resp.json().get('notamList', [])
                if not notam_list:
                    break
                for item in notam_list:
                    raw = item.get('icaoMessage', '')
                    notam_num = item.get('notamNumber', '')
                    series, number, year = _normalize_notam_number(notam_num)
                    lat, lon, radius, qcode = _parse_q_line(raw)
                    n = {
                        'raw': raw,
                        'series': series,
                        'number': number,
                        'year': year,
                        'fir': fir,
                        'from': _parse_faa_time(item.get('startDate')),
                        'to': _parse_faa_time(item.get('endDate')),
                        'latitude': lat,
                        'longitude': lon,
                        'radius': radius,
                        'notamCode': qcode
                    }
                    if not _passes_filters(n):
                        continue
                    rows.append({
                        'id': item.get('transactionID', f"faa-{fir}-{notam_num}"),
                        '_country': 'FAA',
                        'notam': n
                    })

                if len(notam_list) < FAA_PAGE_SIZE:
                    break
                offset += FAA_PAGE_SIZE
            except Exception as e:
                print(f"[faa] Error fetching '{fir}' offset {offset}: {e}")
                break
    print(f"[faa] Supplemental NOTAMs after filter: {len(rows)}")
    return rows

def _item_key(item):
    n = item.get('notam', {})
    series = str(n.get('series', '')).strip().upper()
    number = str(n.get('number', '')).strip()
    year = str(n.get('year', '')).strip()
    if series and number and year:
        return f"NOTAM:{series}{number}/{year}"
    nid = str(item.get('id', '')).strip()
    if nid:
        return f"ID:{nid}"
    raw = str(n.get('raw', '')).strip()
    if raw:
        return f"RAW:{raw[:120]}"
    return ''

def merge_notams(primary, supplemental):
    merged = []
    seen = set()
    for source in (primary, supplemental):
        for item in source:
            key = _item_key(item)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            merged.append(item)
    return merged

# ═══════════════════════════════════════════════════════════════
# notammap.org
# ═══════════════════════════════════════════════════════════════
def fetch_countries():
    url = "https://www.notammap.org/notamdata/countries.json"
    try:
        resp = requests.get(url, headers=make_headers(), timeout=30)
        if resp.status_code == 200:
            countries = resp.json()
            print(f"[notammap] Got {len(countries)} countries.")
            return countries
    except Exception as e:
        print(f"[notammap] Error fetching countries: {e}")
    return []

def fetch_country(country):
    safe_name = country.replace(' ', '_')
    url = f"https://www.notammap.org/notamdata/{safe_name}.json"
    try:
        resp = requests.get(url, headers=make_headers(), timeout=30)
        if resp.status_code == 200:
            return resp.json().get('notams', [])
    except Exception as e:
        print(f"[notammap] Error fetching '{country}': {e}")
    return []

def fetch_notammap():
    countries = fetch_countries()
    if not countries:
        print("[notammap] Failed to get country list!")
        return []

    all_notams = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        future_map = {pool.submit(fetch_country, c): c for c in countries}
        for fut in as_completed(future_map):
            country = future_map[fut]
            try:
                notams = fut.result()
                if notams:
                    for item in notams:
                        item['_country'] = country
                    all_notams.extend(notams)
            except Exception as e:
                print(f"[notammap] Thread error for '{country}': {e}")

    print(f"[notammap] Total raw NOTAMs fetched: {len(all_notams)}")

    # De-duplicate
    seen = set()
    unique = []
    for item in all_notams:
        nid = item.get('id')
        if nid and nid not in seen:
            seen.add(nid)
            unique.append(item)
    print(f"[notammap] Unique NOTAMs: {len(unique)}")

    # KEEP/DROP + time filters
    filtered = []
    for item in unique:
        n = item.get('notam', {})
        if not _passes_filters(n):
            continue
        filtered.append(item)

    print(f"[notammap] After KEEP/DROP + time filter: {len(filtered)}")
    return filtered

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
    ET.SubElement(doc, 'name').text = 'Aerospace NOTAMs'

    for sid, color in [('polyStyle','7700ff00'), ('circleStyle','770000ff'), ('lineStyle','77ff0000')]:
        style = ET.SubElement(doc, 'Style', id=sid)
        ls = ET.SubElement(style, 'LineStyle')
        ET.SubElement(ls, 'color').text = 'ff000000'
        ET.SubElement(ls, 'width').text = '2'
        ps = ET.SubElement(style, 'PolyStyle')
        ET.SubElement(ps, 'color').text = color

    for row in rows:
        lat = row.get('lat', '').strip()
        lon = row.get('lon', '').strip()
        raw = row.get('raw', '')
        notam_id = row.get('notam_id', '')
        fir = row.get('fir', '')
        radius = row.get('radius_nm', '').strip()
        if not lat or not lon:
            continue
        try:
            lat_f, lon_f = float(lat), float(lon)
        except:
            continue

        pm = ET.SubElement(doc, 'Placemark')
        ET.SubElement(pm, 'name').text = notam_id or fir or 'NOTAM'
        ET.SubElement(pm, 'description').text = raw[:500]

        coord_regex = r'(?:([NS])\s*(\d{4,6}(?:[.,]\d+)?))\s*(?:([EW])\s*(\d{5,7}(?:[.,]\d+)?))|(?:(\d{4,6}(?:[.,]\d+)?)\s*([NS]))\s*(?:(\d{5,7}(?:[.,]\d+)?)\s*([EW]))'
        cleaned = re.sub(r'Q\).*?(?=\s*A\))', '', raw, flags=re.DOTALL)
        matches = list(re.finditer(coord_regex, cleaned, re.I))

        if len(matches) >= 3:
            ET.SubElement(pm, 'styleUrl').text = '#polyStyle'
            poly = ET.SubElement(pm, 'Polygon')
            outer = ET.SubElement(poly, 'outerBoundaryIs')
            ring = ET.SubElement(outer, 'LinearRing')
            coords_text = []
            for m in matches:
                if m.group(1):
                    mlat = _parse_coord_val(m.group(2), m.group(1))
                    mlon = _parse_coord_val(m.group(4), m.group(3))
                else:
                    mlat = _parse_coord_val(m.group(5), m.group(6))
                    mlon = _parse_coord_val(m.group(7), m.group(8))
                coords_text.append(f"{mlon},{mlat},0")
            coords_text.append(coords_text[0])
            ET.SubElement(ring, 'coordinates').text = ' '.join(coords_text)
        elif radius and float(radius) > 0:
            ET.SubElement(pm, 'styleUrl').text = '#circleStyle'
            poly = ET.SubElement(pm, 'Polygon')
            outer = ET.SubElement(poly, 'outerBoundaryIs')
            ring = ET.SubElement(outer, 'LinearRing')
            r_deg = float(radius) / 60.0
            coords_text = []
            for i in range(36):
                angle = math.radians(i * 10)
                cx = lon_f + r_deg * math.cos(angle) / math.cos(math.radians(lat_f))
                cy = lat_f + r_deg * math.sin(angle)
                coords_text.append(f"{cx},{cy},0")
            coords_text.append(coords_text[0])
            ET.SubElement(ring, 'coordinates').text = ' '.join(coords_text)
        else:
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
def archive_weekly(csv_path, history_subdir='notams'):
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
    print("Aerospace NOTAM Fetcher (notammap.org)")
    print("=" * 60)

    items = fetch_notammap()
    supplemental = fetch_faa_notams()
    items = merge_notams(items, supplemental)

    # Write notams.csv
    with open('notams.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        for item in items:
            n = item.get('notam', {})
            country = item.get('_country', '')
            nid = item.get('id', '')
            raw = n.get('raw', '').replace('\\n', '  ').replace('\n', '  ').replace('\r', '')
            parts = [n.get('series',''), str(n.get('number','')), str(n.get('year',''))]
            notam_id = f"{parts[0]}{parts[1]}/{parts[2]}" if parts[0] else ''
            fir = n.get('fir', '')
            qcode = n.get('notamCode', '')
            lat = n.get('latitude', '')
            lon = n.get('longitude', '')
            radius = n.get('radius', '')
            if radius == 999:
                radius = ''
            from_utc = n.get('from', '')
            to_utc = n.get('to', '')
            writer.writerow([
                country, str(nid), notam_id, fir, from_utc, to_utc,
                str(lat) if lat != '' else '',
                str(lon) if lon != '' else '',
                str(radius) if radius != '' else '',
                qcode, raw
            ])

    print(f"\nPipeline complete: {len(items)} records written to notams.csv")

    csv_to_kml('notams.csv', 'notams.kml')
    archive_weekly('notams.csv', 'notams')
    print("=" * 60)

if __name__ == '__main__':
    main()

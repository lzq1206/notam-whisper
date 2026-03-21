#!/usr/bin/env python3
"""
Automated aerospace NOTAM fetcher for notam-whisper.
Primary source: https://www.notammap.org/notamdata/
Supplementary: NGA MSI Maritime (msi.nga.mil)
"""
import requests
import re
import os
import glob
import datetime
from datetime import timedelta
import csv
import traceback
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

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
]

now_utc = datetime.datetime.utcnow()
five_days = now_utc + timedelta(days=5)

MSI_NAV_AREAS = ['4', '12', 'A', 'P', 'C']

def make_headers():
    return {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

# ═══════════════════════════════════════════════════════════════
# Source 1 (Primary): notammap.org
# ═══════════════════════════════════════════════════════════════
def fetch_countries():
    """Fetch list of all countries from notammap.org."""
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
    """Fetch all NOTAMs for a single country."""
    safe_name = country.replace(' ', '_')
    url = f"https://www.notammap.org/notamdata/{safe_name}.json"
    try:
        resp = requests.get(url, headers=make_headers(), timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('notams', [])
    except Exception as e:
        print(f"[notammap] Error fetching '{country}': {e}")
    return []

def fetch_notammap():
    """Fetch all NOTAMs from notammap.org, filter by KEEP/DROP keywords."""
    countries = fetch_countries()
    if not countries:
        print("[notammap] Failed to get country list!")
        return []

    all_notams = []
    # Use thread pool to speed up fetching all countries
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

    # De-duplicate by NOTAM id
    seen = set()
    unique = []
    for item in all_notams:
        nid = item.get('id')
        if nid and nid not in seen:
            seen.add(nid)
            unique.append(item)

    print(f"[notammap] Unique NOTAMs: {len(unique)}")

    # Apply KEEP/DROP + time filters
    filtered = []
    for item in unique:
        n = item.get('notam', {})
        raw = n.get('raw', '')
        raw_upper = raw.upper()

        # DROP filter
        if any(d in raw_upper for d in DROP):
            continue
        # KEEP filter
        if not any(k in raw_upper for k in KEEP):
            continue

        # Time filter: keep only NOTAMs active within now..now+5days
        from_str = n.get('from', '')
        to_str = n.get('to', '')
        try:
            if from_str:
                from_dt = datetime.datetime.fromisoformat(from_str.replace('Z', '+00:00')).replace(tzinfo=None)
                if from_dt > five_days:
                    continue
            if to_str:
                to_dt = datetime.datetime.fromisoformat(to_str.replace('Z', '+00:00')).replace(tzinfo=None)
                if to_dt < now_utc:
                    continue
        except:
            pass

        filtered.append(item)

    print(f"[notammap] After KEEP/DROP + time filter: {len(filtered)}")
    return filtered

# ═══════════════════════════════════════════════════════════════
# Source 2 (Supplementary): NGA MSI Maritime Warnings
# ═══════════════════════════════════════════════════════════════
MONTHS_MAP = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
              'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

def msi_coord_to_dd(deg, mm, dec, hemi):
    sec = round(int(dec) * 60 / 100)
    val = int(deg) + int(mm)/60.0 + sec/3600.0
    if hemi in ('S','W'): val = -val
    return val

def parse_msi_cancel_time(text):
    m = re.search(r'CANCEL\s+THIS\s+MSG\s+(\d{2})(\d{4})Z\s+([A-Z]+)\s+(\d{2})', text, re.I)
    if m:
        day, hhmm, mon, yr = m.groups()
        try:
            return datetime.datetime(2000+int(yr), MONTHS_MAP.get(mon.upper(),1), int(day), int(hhmm[:2]), int(hhmm[2:]))
        except: pass
    return None

def parse_msi_coords(text):
    coords = []
    for m in re.finditer(r'(\d{1,2})-(\d{2})\.(\d{2})([NS])\s+(\d{2,3})-(\d{2})\.(\d{2})([EW])', text):
        lat = msi_coord_to_dd(m.group(1), m.group(2), m.group(3), m.group(4))
        lon = msi_coord_to_dd(m.group(5), m.group(6), m.group(7), m.group(8))
        coords.append((lat, lon))
    return coords

def fetch_msi_single(nav_area):
    url = f"https://msi.nga.mil/api/publications/smaps?navArea={nav_area}&status=active&category=14&output=html"
    try:
        resp = requests.get(url, headers=make_headers(), timeout=20)
        if resp.status_code == 200:
            return resp.json().get('smaps', [])
    except Exception as e:
        print(f"[MSI] Error navArea={nav_area}: {e}")
    return []

def fetch_msi():
    all_smaps = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = {pool.submit(fetch_msi_single, na): na for na in MSI_NAV_AREAS}
        for f in as_completed(futs):
            try: all_smaps.extend(f.result())
            except: pass

    rows = []
    seen = set()
    for s in all_smaps:
        cat = s.get('category', '')
        if cat not in ('ROCKET LAUNCHING', 'SPACE DEBRIS'): continue
        msg_id = s.get('msgID', '')
        if msg_id in seen: continue
        seen.add(msg_id)
        msg_text = s.get('msgText', '')
        if not msg_text: continue

        cancel = parse_msi_cancel_time(msg_text)
        if cancel and cancel < now_utc: continue

        coords = parse_msi_coords(msg_text)
        if len(coords) < 3: continue

        msg_type = s.get('msgType', '')
        code_m = re.search(rf'({re.escape(msg_type)}\s+\d+/\d+(?:\([A-Z0-9]+\))?)', msg_text, re.I)
        code = code_m.group(1).strip() if code_m else msg_type

        rows.append({
            'notam_id': code,
            'raw': msg_text,
            'coords': coords,
            'source': 'MSI'
        })

    print(f"[MSI] Found {len(rows)} valid maritime warnings.")
    return rows

# ═══════════════════════════════════════════════════════════════
# Main: Merge all sources into latest.csv
# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
# KML Generation
# ═══════════════════════════════════════════════════════════════
def csv_to_kml(csv_path, kml_path):
    """Convert a latest.csv file into KML format."""
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    kml = ET.Element('kml', xmlns='http://www.opengis.net/kml/2.2')
    doc = ET.SubElement(kml, 'Document')
    ET.SubElement(doc, 'name').text = 'Aerospace NOTAMs'

    # Define styles
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
            lat_f = float(lat)
            lon_f = float(lon)
        except:
            continue

        pm = ET.SubElement(doc, 'Placemark')
        ET.SubElement(pm, 'name').text = notam_id or fir or 'NOTAM'
        ET.SubElement(pm, 'description').text = raw[:500]

        # Try to extract polygon coords from raw text
        coord_regex = r'(?:([NS])\s*(\d{4,6}(?:[.,]\d+)?))\s*(?:([EW])\s*(\d{5,7}(?:[.,]\d+)?))|(?:(\d{4,6}(?:[.,]\d+)?)\s*([NS]))\s*(?:(\d{5,7}(?:[.,]\d+)?)\s*([EW]))'
        cleaned = re.sub(r'Q\).*?(?=\s*A\))', '', raw, flags=re.DOTALL)
        matches = list(re.finditer(coord_regex, cleaned, re.I))

        if len(matches) >= 3:
            # Polygon
            ET.SubElement(pm, 'styleUrl').text = '#polyStyle'
            poly = ET.SubElement(pm, 'Polygon')
            outer = ET.SubElement(poly, 'outerBoundaryIs')
            ring = ET.SubElement(outer, 'LinearRing')
            coords_text = []
            for m in matches:
                if m.group(1):  # N/S first format
                    mlat = _parse_coord_val(m.group(2), m.group(1))
                    mlon = _parse_coord_val(m.group(4), m.group(3))
                else:  # digits first format
                    mlat = _parse_coord_val(m.group(5), m.group(6))
                    mlon = _parse_coord_val(m.group(7), m.group(8))
                coords_text.append(f"{mlon},{mlat},0")
            coords_text.append(coords_text[0])  # close the ring
            ET.SubElement(ring, 'coordinates').text = ' '.join(coords_text)
        elif radius and float(radius) > 0:
            # Circle approximation as polygon
            import math
            ET.SubElement(pm, 'styleUrl').text = '#circleStyle'
            poly = ET.SubElement(pm, 'Polygon')
            outer = ET.SubElement(poly, 'outerBoundaryIs')
            ring = ET.SubElement(outer, 'LinearRing')
            r_deg = float(radius) / 60.0  # NM to degrees (approximate)
            coords_text = []
            for i in range(36):
                angle = math.radians(i * 10)
                cx = lon_f + r_deg * math.cos(angle) / math.cos(math.radians(lat_f))
                cy = lat_f + r_deg * math.sin(angle)
                coords_text.append(f"{cx},{cy},0")
            coords_text.append(coords_text[0])
            ET.SubElement(ring, 'coordinates').text = ' '.join(coords_text)
        else:
            # Simple point
            ET.SubElement(pm, 'styleUrl').text = '#circleStyle'
            point = ET.SubElement(pm, 'Point')
            ET.SubElement(point, 'coordinates').text = f"{lon_f},{lat_f},0"

    tree = ET.ElementTree(kml)
    ET.indent(tree, space='  ')
    tree.write(kml_path, xml_declaration=True, encoding='UTF-8')
    print(f"[KML] Written {len(rows)} placemarks to {kml_path}")

def _parse_coord_val(digits, hemi):
    """Parse DDMM or DDMMSS coordinate string with hemisphere letter to decimal degrees."""
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

# ═══════════════════════════════════════════════════════════════
# Weekly History Archiving
# ═══════════════════════════════════════════════════════════════
def archive_weekly(csv_path):
    """Merge current CSV data into the weekly history file, de-duplicating by notam_id."""
    history_dir = 'history'
    os.makedirs(history_dir, exist_ok=True)

    # Determine current ISO week: YYYY-WNN
    today = datetime.date.today()
    iso_year, iso_week, _ = today.isocalendar()
    week_tag = f"{iso_year}-W{iso_week:02d}"
    weekly_csv = os.path.join(history_dir, f"{week_tag}.csv")
    weekly_kml = os.path.join(history_dir, f"{week_tag}.kml")

    # Read current latest.csv
    new_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        new_rows = list(reader)

    # Read existing weekly CSV if it exists
    existing_rows = []
    if os.path.exists(weekly_csv):
        with open(weekly_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)

    # Merge and de-duplicate by notam_id
    seen = set()
    merged = []
    # New data takes priority (add first)
    for r in new_rows:
        nid = r.get('notam_id', '')
        if nid and nid not in seen:
            seen.add(nid)
            merged.append(r)
        elif not nid:
            merged.append(r)
    # Then add old data that wasn't in new
    for r in existing_rows:
        nid = r.get('notam_id', '')
        if nid and nid not in seen:
            seen.add(nid)
            merged.append(r)
        elif not nid:
            merged.append(r)

    # Write merged weekly CSV
    with open(weekly_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(merged)

    # Generate weekly KML
    csv_to_kml(weekly_csv, weekly_kml)

    print(f"[HISTORY] {week_tag}: {len(merged)} records ({len(new_rows)} new + {len(existing_rows)} existing, {len(merged)} after dedup)")

# ═══════════════════════════════════════════════════════════════
# Main: Merge all sources into latest.csv + latest.kml + history
# ═══════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("Aerospace NOTAM Fetcher - Multi-Source Pipeline")
    print("=" * 60)

    # ── Source 1: notammap.org (Primary) ──
    notammap_items = fetch_notammap()

    # ── Source 2: NGA MSI (Supplementary) ──
    try:
        msi_rows = fetch_msi()
    except Exception as e:
        print(f"[MSI] Source failed: {e}")
        traceback.print_exc()
        msi_rows = []

    # ── Write latest.csv ──
    with open('latest.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw'])

        # notammap.org records
        for item in notammap_items:
            n = item.get('notam', {})
            country = item.get('_country', '')
            nid = item.get('id', '')
            raw = n.get('raw', '').replace('\\n', '  ').replace('\n', '  ').replace('\r', '')
            notam_id_parts = [n.get('series',''), str(n.get('number','')), str(n.get('year',''))]
            notam_id = f"{notam_id_parts[0]}{notam_id_parts[1]}/{notam_id_parts[2]}" if notam_id_parts[0] else ''
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

        # MSI maritime records
        for r in msi_rows:
            coords = r['coords']
            clat = sum(c[0] for c in coords) / len(coords)
            clon = sum(c[1] for c in coords) / len(coords)
            writer.writerow([
                'Maritime', '', r['notam_id'], 'MSI', '', '',
                str(round(clat, 6)), str(round(clon, 6)), '', '',
                r['raw']
            ])

    total = len(notammap_items) + len(msi_rows)
    print(f"\n{'='*60}")
    print(f"Pipeline complete: {total} records written to latest.csv")
    print(f"  notammap.org: {len(notammap_items)} | MSI: {len(msi_rows)}")

    # ── Generate latest.kml ──
    csv_to_kml('latest.csv', 'latest.kml')

    # ── Archive into weekly history ──
    archive_weekly('latest.csv')

    print(f"{'='*60}")

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Automated aerospace NOTAM fetcher for notam-whisper.
Primary source: https://www.notammap.org/notamdata/
Supplementary: NGA MSI Maritime (msi.nga.mil)
"""
import requests
import re
import datetime
from datetime import timedelta
import csv
import traceback
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

    # ── Write CSV ──
    with open('latest.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw'])

        # notammap.org records
        for item in notammap_items:
            n = item.get('notam', {})
            country = item.get('_country', '')
            nid = item.get('id', '')
            raw = n.get('raw', '').replace('\\n', '\n')
            notam_id_parts = [n.get('series',''), str(n.get('number','')), str(n.get('year',''))]
            notam_id = f"{notam_id_parts[0]}{notam_id_parts[1]}/{notam_id_parts[2]}" if notam_id_parts[0] else ''
            fir = n.get('fir', '')
            qcode = n.get('notamCode', '')
            lat = n.get('latitude', '')
            lon = n.get('longitude', '')
            radius = n.get('radius', '')
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
    print(f"{'='*60}")

if __name__ == '__main__':
    main()

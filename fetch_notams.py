#!/usr/bin/env python3
"""
Automated aerospace NOTAM fetcher for notam-whisper.
Data sources:
  1. FAA NOTAM Search (notams.aim.faa.gov) - global ICAO NOTAMs
  2. NGA MSI Maritime (msi.nga.mil) - rocket launches & space debris at sea
  3. China MSA Maritime (msa.gov.cn) - Chinese rocket launch warnings
"""
import requests
import re
import datetime
from datetime import timedelta
import csv
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

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

FAA_SEARCH_TERMS = ["AEROSPACE", "RE-ENTRY", "ROCKET", "SPACE DEBRIS", "DNG ZONE"]
FAA_ICAO_LIST = [
    "ZBPE","ZGZU","ZHWH","ZJSA","ZLHW","ZPKM","ZSHA","ZWUQ","ZYSH","VHHK"
]
MSI_NAV_AREAS = ['4', '12', 'A', 'P', 'C']

now_utc = datetime.datetime.utcnow()
five_days = now_utc + timedelta(days=5)

# ─── Helpers ───
def make_faa_headers():
    return {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

def make_generic_headers():
    return {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

def parse_faa_date(date_str):
    if not date_str: return None
    try: return datetime.datetime.strptime(date_str, "%m/%d/%Y %H%M")
    except: return None

def dms_to_dd(raw, hemi_pos):
    """Convert DDMM[SS] with N/S/E/W hemisphere to decimal degrees."""
    if hemi_pos in ('S','W'):
        sign = -1
    else:
        sign = 1
    raw = str(raw)
    if len(raw) <= 5:  # DDMM or DDDMM
        if hemi_pos in ('E','W'):
            d, m = int(raw[:3]), int(raw[3:5])
            s = 0
        else:
            d, m = int(raw[:2]), int(raw[2:4])
            s = 0
    else:  # DDMMSS or DDDMMSS
        if hemi_pos in ('E','W'):
            d, m, s = int(raw[:3]), int(raw[3:5]), int(raw[5:7])
        else:
            d, m, s = int(raw[:2]), int(raw[2:4]), int(raw[4:6])
    return sign * (d + m/60.0 + s/3600.0)

def parse_qline(msg):
    """Extract lat, lon, radius, fir, qcode from Q-line."""
    lat, lon, rad, fir, qcode = '', '', '', '', ''
    q_match = re.search(
        r'Q\)\s*([A-Z]{4})/([A-Z]{5})/[^/]*/[^/]*/[^/]*/\d{3}/\d{3}/(\d{4}[NS]\d{5}[EW]|\d{6}[NS]\d{7}[EW])(\d{3})',
        msg)
    if q_match:
        fir = q_match.group(1)
        qcode = q_match.group(2)
        coord_str = q_match.group(3)
        rad = str(int(q_match.group(4)))
        try:
            lp = re.match(r'(\d+)([NS])(.*)', coord_str)
            if lp:
                lat_raw, lat_hemi, rest = lp.groups()
                lp2 = re.match(r'(\d+)([EW])', rest)
                if lp2:
                    lon_raw, lon_hemi = lp2.groups()
                    lat = str(round(dms_to_dd(lat_raw, lat_hemi), 6))
                    lon = str(round(dms_to_dd(lon_raw, lon_hemi), 6))
        except: pass
    return lat, lon, rad, fir, qcode

def msi_coord_to_dd(deg, mm, dec, hemi):
    """Convert MSI DD-MM.mm format to decimal degrees."""
    sec = round(int(dec) * 60 / 100)
    val = int(deg) + int(mm)/60.0 + sec/3600.0
    if hemi in ('S','W'): val = -val
    return val

# ═══════════════════════════════════════════════════════════════
# Source 1: FAA NOTAM Search
# ═══════════════════════════════════════════════════════════════
def fetch_faa():
    """Fetch NOTAMs from the FAA public endpoint."""
    session = requests.Session()
    session.headers.update(make_faa_headers())
    url = "https://notams.aim.faa.gov/notamSearch/search"
    results = []

    print("[FAA] Fetching global free-text keywords...")
    for term in FAA_SEARCH_TERMS:
        payload = {"searchType":"4","offset":"0","freeFormText":term,"notamsOnly":"false"}
        for page in range(10):
            payload["offset"] = str(page * 30)
            try:
                resp = session.post(url, data=payload, timeout=15)
                if resp.status_code == 200:
                    notams = resp.json().get('notamList', [])
                    results.extend(notams)
                    if len(notams) < 30: break
            except Exception as e:
                print(f"[FAA] Error '{term}' p{page}: {e}")
                break

    print("[FAA] Fetching specific FIR lists...")
    for icao in FAA_ICAO_LIST:
        payload = {"searchType":"0","designatorsForLocation":icao,"offset":"0","notamsOnly":"false"}
        for page in range(10):
            payload["offset"] = str(page * 30)
            try:
                resp = session.post(url, data=payload, timeout=15)
                if resp.status_code == 200:
                    notams = resp.json().get('notamList', [])
                    results.extend(notams)
                    if len(notams) < 30: break
            except Exception as e:
                print(f"[FAA] Error FIR '{icao}': {e}")
                break

    # De-duplicate and filter
    unique = {}
    for n in results:
        nid = n.get('notamNumber')
        if nid: unique[nid] = n

    filtered = []
    for n in unique.values():
        msg = n.get('icaoMessage', '').upper()
        if any(d in msg for d in DROP): continue
        if not any(k in msg for k in KEEP): continue
        sd = parse_faa_date(n.get('startDate'))
        ed = parse_faa_date(n.get('endDate'))
        if sd and sd > five_days: continue
        if ed and ed < now_utc: continue
        filtered.append(n)

    print(f"[FAA] Filtered to {len(filtered)} valid NOTAMs.")
    return filtered

# ═══════════════════════════════════════════════════════════════
# Source 2: NGA MSI Maritime Warnings
# ═══════════════════════════════════════════════════════════════
def fetch_msi_single(nav_area):
    """Fetch a single navArea from NGA MSI."""
    url = f"https://msi.nga.mil/api/publications/smaps?navArea={nav_area}&status=active&category=14&output=html"
    try:
        resp = requests.get(url, headers=make_generic_headers(), timeout=20)
        if resp.status_code == 200:
            return resp.json().get('smaps', [])
    except Exception as e:
        print(f"[MSI] Error navArea={nav_area}: {e}")
    return []

def parse_msi_coords(text):
    """Extract DD-MM.mmN DDD-MM.mmW coordinates from MSI text."""
    coords = []
    for m in re.finditer(r'(\d{1,2})-(\d{2})\.(\d{2})([NS])\s+(\d{2,3})-(\d{2})\.(\d{2})([EW])', text):
        lat = msi_coord_to_dd(m.group(1), m.group(2), m.group(3), m.group(4))
        lon = msi_coord_to_dd(m.group(5), m.group(6), m.group(7), m.group(8))
        coords.append((lat, lon))
    return coords

MONTHS_MAP = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
              'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

def parse_msi_cancel_time(text):
    """Parse CANCEL THIS MSG DDHHMMZ MON YY."""
    m = re.search(r'CANCEL\s+THIS\s+MSG\s+(\d{2})(\d{4})Z\s+([A-Z]+)\s+(\d{2})', text, re.I)
    if m:
        day, hhmm, mon, yr = m.groups()
        try:
            return datetime.datetime(2000+int(yr), MONTHS_MAP.get(mon.upper(),1), int(day), int(hhmm[:2]), int(hhmm[2:]))
        except: pass
    return None

def fetch_msi():
    """Fetch rocket launch / space debris warnings from NGA MSI."""
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
# Source 3: China MSA Maritime (msa.gov.cn)
# ═══════════════════════════════════════════════════════════════
def parse_msa_coordinates(text):
    """Parse Chinese maritime coordinate formats."""
    text = re.sub(r'\s+', ' ', text).strip()
    text_ns = text.replace(' ', '')
    coords = []
    for pat in [
        r'(\d{1,3})-(\d{1,2})\.(\d{1,2})([NS])\s+(\d{1,3})-(\d{1,2})\.(\d{1,2})([EW])',
        r'(\d{1,3})-(\d{1,2})\.(\d{1,2})([NS])/(\d{1,3})-(\d{1,2})\.(\d{1,2})([EW])',
        r'(\d{1,3})-(\d{1,2})\.(\d{1,2})([NS])(\d{1,3})-(\d{1,2})\.(\d{1,2})([EW])',
    ]:
        for m in re.finditer(pat, text if '/' not in pat else text, re.I):
            lat = msi_coord_to_dd(m.group(1), m.group(2), m.group(3), m.group(4))
            lon = msi_coord_to_dd(m.group(5), m.group(6), m.group(7), m.group(8))
            coords.append((lat, lon))
        if coords: break
    return coords

def fetch_msa():
    """Fetch rocket launch NOTAMs from China Maritime Safety Administration."""
    rows = []
    base_url = "https://www.msa.gov.cn"
    index_url = f"{base_url}/page/channelArticles.do?channelids=9C219298-B27F-460E-995A-99401B3FF6AF"

    try:
        resp = requests.get(index_url, headers=make_generic_headers(), timeout=15)
        resp.encoding = 'utf-8'
        if resp.status_code != 200:
            print(f"[MSA] Index page failed: {resp.status_code}")
            return rows

        soup = BeautifulSoup(resp.text, 'html.parser')
        rocket_links = []
        for li in soup.find_all('li'):
            link = li.find('a', href=True)
            if link:
                span = link.find('span')
                if span and '火箭' in span.get_text():
                    href = link.get('href')
                    title = span.get_text().strip()
                    pub_date = datetime.datetime.now().strftime('%Y-%m-%d')
                    for s in li.find_all('span'):
                        dm = re.search(r'\[(\d{4}-\d{2}-\d{2})\]', s.get_text())
                        if dm: pub_date = dm.group(1); break
                    rocket_links.append({'href': href, 'title': title, 'pub_date': pub_date})

        print(f"[MSA] Found {len(rocket_links)} rocket-related links.")

        for lnk in rocket_links:
            href = lnk['href']
            detail_url = (base_url + href) if href.startswith('/') else href
            try:
                dr = requests.get(detail_url, headers=make_generic_headers(), timeout=15)
                dr.encoding = 'utf-8'
                if dr.status_code != 200: continue
                ds = BeautifulSoup(dr.text, 'html.parser')
                content_div = ds.find('div', {'class': 'text', 'id': 'ch_p'})
                if not content_div: continue
                raw_text = re.sub(r'\s+', ' ', content_div.get_text(separator=' ', strip=True))
                raw_text = re.sub(r'收藏.*?关闭窗口', '', raw_text)

                coords = parse_msa_coordinates(raw_text)
                if len(coords) < 3: continue

                code_m = re.search(r'([沪浙苏鲁粤闽琼桂辽冀津京深港澳台渤黄东南]{1,2}航警\d+/\d+)', raw_text)
                code = code_m.group(1) if code_m else lnk['title']

                rows.append({
                    'notam_id': code,
                    'raw': raw_text,
                    'coords': coords,
                    'source': 'MSA'
                })
                print(f"[MSA] Parsed: {code}")
            except Exception as e:
                print(f"[MSA] Detail error: {e}")

    except Exception as e:
        print(f"[MSA] Fetch error: {e}")
        traceback.print_exc()

    print(f"[MSA] Found {len(rows)} valid maritime warnings.")
    return rows

# ═══════════════════════════════════════════════════════════════
# Main: Merge all sources into latest.csv
# ═══════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("Aerospace NOTAM Fetcher - Multi-Source Pipeline")
    print("=" * 60)

    # ── Source 1: FAA ──
    faa_notams = fetch_faa()

    # ── Source 2: NGA MSI ──
    try:
        msi_rows = fetch_msi()
    except Exception as e:
        print(f"[MSI] Source failed: {e}")
        msi_rows = []

    # ── Source 3: China MSA ──
    try:
        msa_rows = fetch_msa()
    except Exception as e:
        print(f"[MSA] Source failed: {e}")
        msa_rows = []

    # ── Write CSV ──
    with open('latest.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw'])

        # FAA records
        for n in faa_notams:
            icao_msg = n.get('icaoMessage', '')
            trad_msg = n.get('traditionalMessage', '')
            msg = icao_msg if trad_msg in icao_msg else f"{icao_msg}\n{trad_msg}"
            notam_id = n.get('notamNumber', '')
            fir = n.get('facilityDesignator', '')
            from_utc, to_utc = '', ''
            sd = parse_faa_date(n.get('startDate'))
            if sd: from_utc = sd.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            ed = parse_faa_date(n.get('endDate'))
            if ed: to_utc = ed.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            lat, lon, rad, fir2, qcode = parse_qline(msg)
            if fir2: fir = fir2
            writer.writerow(['Global', n.get('transactionID',''), notam_id, fir, from_utc, to_utc, lat, lon, rad, qcode, msg])

        # MSI records (maritime rocket/space debris)
        for r in msi_rows:
            coords = r['coords']
            # Use centroid as lat/lon
            clat = sum(c[0] for c in coords) / len(coords)
            clon = sum(c[1] for c in coords) / len(coords)
            writer.writerow([
                'Maritime', '', r['notam_id'], 'MSI', '', '',
                str(round(clat, 6)), str(round(clon, 6)), '', '',
                r['raw']
            ])

        # MSA records (China maritime rocket launches)
        for r in msa_rows:
            coords = r['coords']
            clat = sum(c[0] for c in coords) / len(coords)
            clon = sum(c[1] for c in coords) / len(coords)
            writer.writerow([
                'China', '', r['notam_id'], 'MSA', '', '',
                str(round(clat, 6)), str(round(clon, 6)), '', '',
                r['raw']
            ])

    total = len(faa_notams) + len(msi_rows) + len(msa_rows)
    print(f"\n{'='*60}")
    print(f"Pipeline complete: {total} records written to latest.csv")
    print(f"  FAA: {len(faa_notams)} | MSI: {len(msi_rows)} | MSA: {len(msa_rows)}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()

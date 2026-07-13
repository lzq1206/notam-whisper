#!/usr/bin/env python3
"""
Aerospace NOTAM fetcher — notammap.org + FAA + global supplement.
Outputs: notams.csv, notams.kml, history/notams/YYYY-WNN.*
"""
import requests, re, os, sys, datetime, csv, math, time, unicodedata, html as html_lib
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET
from urllib.parse import quote

# ─── Keyword Filters ───
KEEP = ["UNL", "AEROSPACE", "RE-ENTRY", "ROCKET", "MISSILE", "SPACE", "SPACEFLIGHT", "SATELLITE"]
KEEP_QCODES = {"QRDCA", "QWMLW", "QWELW"}
KEEP_PATTERNS = [re.compile(rf"(?<![A-Z0-9]){re.escape(keyword)}(?![A-Z0-9])") for keyword in KEEP]
DROP = [
    "KWAJALEIN","BALLOON","BALLON","TRANSMITTER","GUNFIRING","AERIAL","GUN FRNG",
    "AIR EXER","REF AIP","KOLKATA","MWARA","ZS(D)","ZY(R)","ZG(R)",
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

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw']
FAA_SEARCH_URL = "https://notams.aim.faa.gov/notamSearch/search"
FAA_SUPPLEMENTAL_FIRS = [
    # China / Hong Kong / Philippines (existing coverage)
    "ZBPE", "ZGZU", "ZHWH", "ZJSA", "ZLHW", "ZPKM", "ZSHA", "ZWUQ", "ZYSH",
    "VHHK", "RPHI", "ZXXX",
    # Global launch / re-entry corridors.  The previous supplemental list was
    # China-heavy, so FAA fallback searches mostly added China-nearby NOTAMs
    # when notammap missed items.  Keep this list targeted to major launch and
    # oceanic FIRs to avoid broad crawling / rate pressure.
    "KZAK", "KZLA", "KZMA", "KZNY", "KZHU", "KZJX", "PAZA",
    "MMFR", "SBAO", "SBCW", "SBAZ", "SOOO",
    "GMMM", "GCCC", "LPPO", "EGTT", "EGPX", "EISN",
    "ENOR", "ENOB", "ESAA", "ESOS", "EFIN",
    "LFFF", "LFBB", "LFMM", "LSAS", "EDWW",
    "LTAA", "OJAC", "LLLL", "OIIX", "OAKX",
    "UAAA", "UATT", "UCFM", "UHHH", "UHPP", "UHMM", "UWWW", "URRV", "UUWV",
    "RJTG", "RJJJ", "RKRR", "VOMF", "VABF", "VIDF",
    "YMMM", "NZZC", "NFFF", "AYPM",
    "FACT", "FAJO", "FMMM", "FIMM", "FSSS",
]
FAA_PAGE_SIZE = 30
FAA_MAX_PAGES_PER_FIR = 2
FAA_REQUEST_DELAY_SECONDS = 0.35
MAX_FUTURE_DAYS = 30
NOTAMMAP_MAX_WORKERS = 10
COUNTRY_FETCH_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
GLOBAL_SUPPLEMENT_URL = "https://raw.githubusercontent.com/Joey0609/notams/main/notams/notam_data.json"
GLOBAL_SUPPLEMENT_TYPES = {"launch", "missile", "reentry"}
FAA_TFR_LIST_URL = "https://tfr.faa.gov/tfrapi/getTfrList"
FAA_TFR_DETAIL_URL = "https://tfr.faa.gov/tfrapi/getWebText"
FAA_TFR_MAX_DETAILS = 20
FAA_TFR_REQUEST_DELAY_SECONDS = 0.35

def make_headers():
    return {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

def _is_in_time_window(from_str, to_str):
    """Return True if the record is not expired and not too far in the future."""
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    five_days = now_utc + timedelta(days=MAX_FUTURE_DAYS)
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

def _passes_filters(notam):
    raw = notam.get('raw', '')
    raw_upper = raw.upper()
    qcode = str(notam.get('notamCode', '')).strip().upper()
    if any(d in raw_upper for d in DROP):
        return False
    # Use token boundaries: substring matching made SPACE match every AIRSPACE
    # notice and UNL match words such as UNLIT/UNLIGHTED.
    if not any(pattern.search(raw_upper) for pattern in KEEP_PATTERNS) and qcode not in KEEP_QCODES:
        return False
    from_str = notam.get('from', '')
    to_str = notam.get('to', '')
    return _is_in_time_window(from_str, to_str)

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

def _parse_notam_time(code):
    code = str(code or '').strip()
    if not code or code.upper() == 'PERM':
        return ''
    try:
        dt = datetime.datetime.strptime(code, "%y%m%d%H%M")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ''

def _extract_notam_id(raw, fallback=''):
    raw = str(raw or '')
    m = re.search(r'\b([A-Z]\d{1,4}/\d{2})\b', raw)
    if m:
        return m.group(1).upper()
    return str(fallback or '').strip().upper()

def _parse_faa_tfr_time(text):
    text = str(text or '').strip()
    try:
        dt = datetime.datetime.strptime(text, "%B %d, %Y at %H%M UTC")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return ''

def _html_to_text(value):
    text = html_lib.unescape(str(value or ''))
    text = re.sub(r'<[^>]+>', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()

def _normalize_faa_tfr_coordinates(text):
    """Convert FAA display DMS pairs to compact ICAO coordinates."""
    pattern = re.compile(
        r'(\d{1,2})[º°](\d{2})[\'’](\d{2})["”]?([NS])\s+'
        r'(\d{1,3})[º°](\d{2})[\'’](\d{2})["”]?([EW])',
        re.I,
    )
    points = []

    def replace(match):
        lat_d, lat_m, lat_s, lat_h, lon_d, lon_m, lon_s, lon_h = match.groups()
        lat_compact = f"{int(lat_d):02d}{lat_m}{lat_s}{lat_h.upper()}"
        lon_compact = f"{int(lon_d):03d}{lon_m}{lon_s}{lon_h.upper()}"
        lat = _parse_coord_val(f"{int(lat_d):02d}{lat_m}{lat_s}", lat_h)
        lon = _parse_coord_val(f"{int(lon_d):03d}{lon_m}{lon_s}", lon_h)
        points.append((lat, lon))
        return f"{lat_compact} {lon_compact}"

    normalized = pattern.sub(replace, text)
    return normalized, points

def _extract_labeled_tfr_value(text, label, next_label):
    match = re.search(
        rf'{re.escape(label)}\s*:\s*(.*?)\s+{re.escape(next_label)}\s*:',
        text,
        re.I,
    )
    return match.group(1).strip() if match else ''

def _parse_faa_tfr_detail(summary, detail_payload):
    records = detail_payload if isinstance(detail_payload, list) else []
    if not records or not isinstance(records[0], dict):
        return None
    text = _html_to_text(records[0].get('text', ''))
    if not text:
        return None

    begin_text = _extract_labeled_tfr_value(text, 'Beginning Date and Time', 'Ending Date and Time')
    end_text = _extract_labeled_tfr_value(text, 'Ending Date and Time', 'Reason for NOTAM')
    from_utc = _parse_faa_tfr_time(begin_text)
    to_utc = _parse_faa_tfr_time(end_text)
    normalized_text, points = _normalize_faa_tfr_coordinates(text)
    if not _is_in_time_window(from_utc, to_utc):
        return None

    notam_id = str(summary.get('notam_id', '') or records[0].get('notam_id', '')).strip()
    facility = str(summary.get('facility', '') or '').strip().upper()
    description = str(summary.get('description', '') or '').strip()
    raw = (
        f"FDC {notam_id} AIRSPACE SPACE OPERATIONS TFR. {description}. "
        f"{normalized_text}"
    ).strip()
    lat = round(sum(p[0] for p in points) / len(points), 6) if points else ''
    lon = round(sum(p[1] for p in points) / len(points), 6) if points else ''
    return {
        'id': f"faa-tfr-{notam_id}",
        '_country': 'USA',
        'notam': {
            'notam_id': f"FDC {notam_id}",
            'raw': raw,
            'series': '',
            'number': '',
            'year': '',
            'fir': facility,
            'from': from_utc,
            'to': to_utc,
            'latitude': lat,
            'longitude': lon,
            'radius': '',
            'notamCode': 'TFR91.143',
        },
    }

def fetch_faa_space_tfrs():
    """Fetch active FAA space-operation TFRs with a tightly bounded request count."""
    headers = make_headers()
    try:
        response = requests.get(FAA_TFR_LIST_URL, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"[faa-tfr] List unavailable: HTTP {response.status_code}")
            return []
        summaries = response.json()
        if not isinstance(summaries, list):
            print("[faa-tfr] Unexpected list response format")
            return []
    except Exception as exc:
        print(f"[faa-tfr] Error fetching list: {exc}")
        return []

    candidates = [
        row for row in summaries
        if isinstance(row, dict) and str(row.get('type', '')).strip().upper() == 'SPACE OPERATIONS'
    ][:FAA_TFR_MAX_DETAILS]
    rows = []
    for index, summary in enumerate(candidates):
        notam_id = str(summary.get('notam_id', '')).strip()
        if not notam_id:
            continue
        if index:
            time.sleep(FAA_TFR_REQUEST_DELAY_SECONDS)
        try:
            response = requests.get(
                FAA_TFR_DETAIL_URL,
                params={'notamId': notam_id},
                headers=headers,
                timeout=30,
            )
            if response.status_code != 200:
                print(f"[faa-tfr] Detail {notam_id} unavailable: HTTP {response.status_code}")
                continue
            item = _parse_faa_tfr_detail(summary, response.json())
            if item:
                rows.append(item)
        except Exception as exc:
            print(f"[faa-tfr] Error fetching {notam_id}: {exc}")
    print(f"[faa-tfr] Active space-operation TFRs: {len(rows)}")
    return rows

def fetch_global_notam_supplement():
    rows = []
    try:
        resp = requests.get(GLOBAL_SUPPLEMENT_URL, headers=make_headers(), timeout=45)
        if resp.status_code != 200:
            print(f"[global] Non-200 response: HTTP {resp.status_code}")
            return rows
        data = resp.json()
    except Exception as e:
        print(f"[global] Error fetching supplement: {e}")
        return rows

    feats = data.get('features', []) if isinstance(data, dict) else []
    for feat in feats:
        if feat.get('eventType') not in GLOBAL_SUPPLEMENT_TYPES:
            continue
        raw = str(feat.get('description', '') or '')
        if not raw:
            continue
        notam_id = _extract_notam_id(raw, feat.get('name', ''))
        q_m = re.search(r'Q\)\s*([A-Z0-9]{4})/([A-Z0-9]{5})/', raw)
        fir = q_m.group(1).strip() if q_m else str(feat.get('site', '') or '')
        qcode = q_m.group(2).strip() if q_m else ''
        lat, lon, radius, parsed_qcode = _parse_q_line(raw)
        if not qcode:
            qcode = parsed_qcode
        b_m = re.search(r'\bB\)\s*([0-9]{10}|PERM)\b', raw)
        c_m = re.search(r'\bC\)\s*([0-9]{10}|PERM)\b', raw)
        from_utc = _parse_notam_time(b_m.group(1)) if b_m else ''
        to_utc = _parse_notam_time(c_m.group(1)) if c_m else ''
        n = {
            'raw': raw,
            'series': notam_id[:1] if notam_id else '',
            'number': int(notam_id[1:].split('/')[0]) if notam_id and '/' in notam_id and notam_id[1:].split('/')[0].isdigit() else '',
            'year': f"20{notam_id.split('/')[-1]}" if notam_id and '/' in notam_id and len(notam_id.split('/')[-1]) == 2 else '',
            'fir': fir,
            'from': from_utc,
            'to': to_utc,
            'latitude': lat,
            'longitude': lon,
            'radius': radius,
            'notamCode': qcode,
        }
        if not _passes_filters(n):
            continue
        rows.append({
            'id': feat.get('id', notam_id or feat.get('name', '')),
            '_country': str(feat.get('country', 'GLOBAL') or 'GLOBAL'),
            'notam': n,
        })
    print(f"[global] Supplemental NOTAMs after filter: {len(rows)}")
    return rows

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
        pages_fetched = 0
        while pages_fetched < FAA_MAX_PAGES_PER_FIR:
            payload = {
                "searchType": "0",
                "designatorsForLocation": fir,
                "offset": str(offset),
                "notamsOnly": "false"
            }
            try:
                resp = session.post(FAA_SEARCH_URL, data=payload, timeout=30)
                if resp.status_code != 200:
                    print(f"[faa] Non-200 response for '{fir}' offset {offset}: HTTP {resp.status_code}")
                    break
                try:
                    data = resp.json()
                except ValueError as e:
                    print(f"[faa] Non-JSON response for '{fir}' offset {offset}: {e}")
                    break
                notam_list = data.get('notamList', [])
                if not notam_list:
                    break
                pages_fetched += 1
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
                time.sleep(FAA_REQUEST_DELAY_SECONDS)
            except Exception as e:
                print(f"[faa] Error fetching '{fir}' offset {offset}: {e}")
                break
        time.sleep(FAA_REQUEST_DELAY_SECONDS)
    print(f"[faa] Supplemental NOTAMs after filter: {len(rows)}")
    return rows

def _item_key(item):
    n = item.get('notam', {})
    display_id = str(n.get('notam_id', '')).strip().upper()
    if display_id:
        return f"NOTAM:{display_id}"
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

def fetch_country(country, report_failure=False):
    raw_name = country.strip()
    if not raw_name:
        return None if report_failure else []

    variants = []
    seen_variants = set()

    def _add_variant(name):
        cleaned = re.sub(r'__+', '_', name.strip('_'))
        if not cleaned or cleaned in seen_variants:
            return
        seen_variants.add(cleaned)
        variants.append(cleaned)

    base = raw_name.replace(' ', '_')
    _add_variant(base)

    ascii_base = unicodedata.normalize('NFKD', base).encode('ascii', 'ignore').decode('ascii')
    _add_variant(ascii_base)

    for name in list(variants):
        _add_variant(name.replace("'", ''))
        _add_variant(name.replace("'", '_'))
        _add_variant(name.replace('’', ''))
        _add_variant(name.replace('’', '_'))

    for attempt in range(1, COUNTRY_FETCH_RETRIES + 1):
        should_retry = False
        last_status = None
        try:
            for idx, safe_name in enumerate(variants):
                encoded_name = quote(safe_name)
                url = f"https://www.notammap.org/notamdata/{encoded_name}.json"
                resp = requests.get(url, headers=make_headers(), timeout=30)
                last_status = resp.status_code
                if resp.status_code == 200:
                    return resp.json().get('notams', [])
                if resp.status_code == 404 and idx < len(variants) - 1:
                    continue
                if resp.status_code in RETRYABLE_STATUS_CODES:
                    should_retry = True
                    print(f"[notammap] Retryable status for '{country}': HTTP {resp.status_code} (attempt {attempt}/{COUNTRY_FETCH_RETRIES})")
                    break
                print(f"[notammap] Error fetching '{country}': HTTP {resp.status_code}")
                return None if report_failure else []
        except Exception as e:
            print(f"[notammap] Error fetching '{country}' (attempt {attempt}/{COUNTRY_FETCH_RETRIES}): {e}")
            should_retry = True

        if should_retry and attempt < COUNTRY_FETCH_RETRIES:
            time.sleep(attempt)
            continue

        if last_status is not None:
            print(f"[notammap] Error fetching '{country}': HTTP {last_status}")
        break
    return None if report_failure else []

def fetch_notammap():
    countries = fetch_countries()
    if not countries:
        print("[notammap] FATAL: Unable to reach notammap.org (empty country list). "
              "Aborting to preserve existing data.")
        sys.exit(1)

    all_notams = []
    failed_countries = []
    with ThreadPoolExecutor(max_workers=NOTAMMAP_MAX_WORKERS) as pool:
        future_map = {pool.submit(fetch_country, c, True): c for c in countries}
        for fut in as_completed(future_map):
            country = future_map[fut]
            try:
                notams = fut.result()
                if notams is None:
                    failed_countries.append(country)
                    continue
                if notams:
                    for item in notams:
                        item['_country'] = country
                    all_notams.extend(notams)
            except Exception as e:
                print(f"[notammap] Thread error for '{country}': {e}")

    if failed_countries:
        print(f"[notammap] Retrying failed countries sequentially: {len(failed_countries)}")
        for country in failed_countries:
            notams = fetch_country(country, True)
            if notams is None:
                print(f"[notammap] Giving up on '{country}' after retries.")
                continue
            if notams:
                for item in notams:
                    item['_country'] = country
                all_notams.extend(notams)

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
        if not _is_in_time_window(r.get('from_utc', ''), r.get('to_utc', '')):
            continue
        nid = r.get('notam_id', '')
        if nid and nid not in seen:
            seen.add(nid)
            merged.append(r)
        elif not nid:
            merged.append(r)
    for r in existing_rows:
        if not _is_in_time_window(r.get('from_utc', ''), r.get('to_utc', '')):
            continue
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
    faa_tfrs = fetch_faa_space_tfrs()
    global_supplement = fetch_global_notam_supplement()
    items = merge_notams(items, supplemental)
    items = merge_notams(items, faa_tfrs)
    items = merge_notams(items, global_supplement)

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
            notam_id = n.get('notam_id', '') or (f"{parts[0]}{parts[1]}/{parts[2]}" if parts[0] else '')
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

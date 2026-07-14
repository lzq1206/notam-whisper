#!/usr/bin/env python3
"""
Aerospace NOTAM fetcher — notammap.org + FAA + global supplement.
Outputs: notams.csv, notams.kml, history/notams/YYYY-WNN.*
"""
import requests, re, os, sys, datetime, csv, math, time, json, unicodedata, html as html_lib
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET
from urllib.parse import quote

# ─── Keyword Filters ───
KEEP_PATTERNS = [
    re.compile(pattern, re.I) for pattern in (
        r'\bAEROSPACE\b',
        r'\bSPACE\s+OPERATIONS?\b',
        r'\bSPACE\s+LAUNCH\b',
        r'\bSPACE\s+ACTI?VITIES?\b',
        r'\bSPACEFLIGHT\b',
        r'\bRE[ -]?ENTRY\b',
        r'\bROCKET(?:S|RY)?\b',
        r'\bBALLISTIC\s+MISSILE\b',
        r'\bMISSILE(?:S)?\s+(?:FIRING|LAUNCH|TEST|ACTIVITY|OPERATIONS?)\b',
        r'\bLAUNCH\s+(?:VEHICLE|ACTIVITY|AREA|WINDOW|OPERATIONS?)\b',
        r'\bSATELLITE\s+LAUNCH\b',
        r'\b(?:UNBURNED\s+)?DEBRIS\b',
        r'\bFALL\s+AREA\b',
        r'\bSPLASHDOWN\b',
    )
]
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

CSV_HEADERS = ['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw','polygon']
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
    "UACN", "UAAA", "UATT", "UCFM", "UHHH", "UHPP", "UHMM", "UWWW", "URRV", "UUWV",
    "RJTG", "RJJJ", "RKRR", "VOMF", "VABF", "VIDF",
    "YMMM", "NZZC", "NFFF", "AYPM",
    "FACT", "FAJO", "FMMM", "FIMM", "FSSS",
]
FAA_PAGE_SIZE = 30
FAA_MAX_PAGES_PER_FIR = 2
FAA_MAX_PAGES_FOR_ACTIVE_LAUNCH_FIR = 3
FAA_REQUEST_DELAY_SECONDS = 0.35
MAX_FUTURE_DAYS = 30
NOTAMMAP_MAX_WORKERS = 10
COUNTRY_FETCH_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
GLOBAL_SUPPLEMENT_URL = "https://raw.githubusercontent.com/Joey0609/notams/main/data_dict.json"
GLOBAL_SUPPLEMENT_TYPES = {"launch", "missile", "reentry"}
FAA_TFR_LIST_URL = "https://tfr.faa.gov/tfrapi/getTfrList"
FAA_TFR_DETAIL_URL = "https://tfr.faa.gov/tfrapi/getWebText"
FAA_TFR_MAX_DETAILS = 20
FAA_TFR_REQUEST_DELAY_SECONDS = 0.35
UPCOMING_LAUNCH_URL = "https://fdo.rocketlaunch.live/json/launches/next/5"
SILENT_LAUNCH_QCODES = {"QRPCA", "QRDCA"}
SILENT_LAUNCH_MIN_CEILING_FL = 300
SILENT_LAUNCH_TIME_TOLERANCE_MINUTES = 20
SILENT_LAUNCH_MAX_DISTANCE_NM = 1000
LAUNCH_SITE_FIRS = {
    'baikonur cosmodrome': {'UACN', 'UAAA', 'UATT'},
}

def make_headers():
    return {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

def _parse_iso_datetime(value):
    try:
        return datetime.datetime.fromisoformat(str(value or '').replace('Z', '+00:00')).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None

def _load_launch_site_coordinates():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'launch_sites.csv')
    sites = {}
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            for row in csv.DictReader(handle):
                name = str(row.get('name', '') or '').strip().casefold()
                if not name:
                    continue
                try:
                    sites[name] = (float(row['latitude']), float(row['longitude']))
                except (KeyError, TypeError, ValueError):
                    continue
    except OSError as exc:
        print(f"[launch-context] Unable to read launch_sites.csv: {exc}")
    return sites

def fetch_upcoming_launch_context():
    """Return upcoming launches with a known site and exact UTC launch time."""
    try:
        response = requests.get(UPCOMING_LAUNCH_URL, headers=make_headers(), timeout=30)
        if response.status_code != 200:
            print(f"[launch-context] Upcoming launch feed unavailable: HTTP {response.status_code}")
            return []
        payload = response.json()
    except Exception as exc:
        print(f"[launch-context] Error fetching upcoming launches: {exc}")
        return []

    sites = _load_launch_site_coordinates()
    records = payload.get('result', []) if isinstance(payload, dict) else []
    contexts = []
    for record in records:
        if not isinstance(record, dict):
            continue
        launch_time = _parse_iso_datetime(record.get('t0'))
        location = ((record.get('pad') or {}).get('location') or {})
        location_name = str(location.get('name', '') or '').strip()
        coordinates = sites.get(location_name.casefold())
        if not launch_time or not coordinates:
            continue
        contexts.append({
            'mission': str(record.get('name', '') or '').strip(),
            'time': launch_time,
            'site': location_name,
            'lat': coordinates[0],
            'lon': coordinates[1],
        })
    print(f"[launch-context] Upcoming launches with mapped sites: {len(contexts)}")
    return contexts

def _great_circle_distance_nm(origin, destination):
    lat1, lon1 = map(math.radians, origin)
    lat2, lon2 = map(math.radians, destination)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 3440.065 * 2 * math.atan2(math.sqrt(value), math.sqrt(max(0.0, 1.0 - value)))

def _extract_raw_coordinate_points(raw):
    pattern = re.compile(r'(\d{6})([NS])\s+(\d{7})([EW])', re.I)
    return [
        [_parse_coord_val(match.group(1), match.group(2)),
         _parse_coord_val(match.group(3), match.group(4))]
        for match in pattern.finditer(str(raw or ''))
    ]

def _build_straight_line_corridor_polygon(raw):
    """Buffer ICAO `NM EITHER SIDE OF A STRAIGHT LINE` geometry."""
    match = re.search(
        r'(\d+(?:\.\d+)?)\s*NM\s+EITHER\s+SIDE\s+OF\s+A\s+STRAIGHT\s+LINE\s+'
        r'DEFINED\s+BY\s*:(.*?)(?:\s+F\)|$)',
        str(raw or ''),
        re.I | re.S,
    )
    if not match:
        return []
    width_nm = float(match.group(1))
    points = _extract_raw_coordinate_points(match.group(2))
    if len(points) < 2:
        return []

    left, right = [], []
    for index, point in enumerate(points):
        before = points[max(0, index - 1)]
        after = points[min(len(points) - 1, index + 1)]
        bearing = _faa_bearing_deg(before, after)
        left.append(_faa_destination_point(point, bearing - 90.0, width_nm))
        right.append(_faa_destination_point(point, bearing + 90.0, width_nm))
    return left + list(reversed(right))

def _has_high_launch_ceiling(notam):
    raw = str(notam.get('raw', '') or '')
    if re.search(r'\bG\)\s*UNL\b', raw, re.I):
        return True
    qline = re.search(r'Q\)\s*[A-Z0-9]{4}/[A-Z0-9]{5}/[^/]+/[^/]+/[^/]+/(\d{3})/(\d{3})/', raw, re.I)
    return bool(qline and int(qline.group(2)) >= SILENT_LAUNCH_MIN_CEILING_FL)

def _schedule_allows_time(raw, event_time):
    """Honor common ICAO D-line schedules instead of the broad B/C envelope."""
    match = re.search(r'\bD\)\s*(.*?)\s+E\)', str(raw or ''), re.I | re.S)
    if not match:
        return True
    schedule = re.sub(r'\s+', ' ', match.group(1)).strip().upper()
    hhmm = event_time.hour * 100 + event_time.minute

    daily = re.fullmatch(r'DAILY\s+(\d{4})-(\d{4})', schedule)
    if daily:
        return int(daily.group(1)) <= hhmm <= int(daily.group(2))

    month_names = {name.upper(): number for number, name in enumerate(
        ('', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
         'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC')
    ) if name}
    dated_windows = re.findall(r'([A-Z]{3})\s+(\d{1,2})\s+(\d{4})-(\d{4})', schedule)
    if dated_windows:
        for month, day, start, end in dated_windows:
            if month_names.get(month) == event_time.month and int(day) == event_time.day:
                return int(start) <= hhmm <= int(end)
        return False

    # Complex weekly/conditional schedules are not sufficiently precise for
    # automatic launch correlation; explicit aerospace wording can still pass.
    return False

def _correlate_silent_launch_notam(notam, launch_contexts):
    """Trust otherwise-generic danger/prohibited areas only with launch evidence."""
    qcode = str(notam.get('notamCode', '') or '').strip().upper()
    if qcode not in SILENT_LAUNCH_QCODES or not _has_high_launch_ceiling(notam):
        return ''
    begin = _parse_iso_datetime(notam.get('from'))
    end = _parse_iso_datetime(notam.get('to'))
    if not begin or not end:
        return ''

    raw = str(notam.get('raw', '') or '')
    points = _extract_raw_coordinate_points(raw)
    if not points and notam.get('latitude') != '' and notam.get('longitude') != '':
        try:
            points = [[float(notam['latitude']), float(notam['longitude'])]]
        except (TypeError, ValueError):
            points = []
    tolerance = timedelta(minutes=SILENT_LAUNCH_TIME_TOLERANCE_MINUTES)
    for launch in launch_contexts or []:
        launch_time = launch.get('time')
        if not isinstance(launch_time, datetime.datetime):
            continue
        if launch_time < begin - tolerance or launch_time > end + tolerance:
            continue
        if not _schedule_allows_time(raw, launch_time):
            continue
        site = (float(launch['lat']), float(launch['lon']))
        if not points or min(_great_circle_distance_nm(site, point) for point in points) > SILENT_LAUNCH_MAX_DISTANCE_NM:
            continue
        notam['_trusted_launch_source'] = True
        notam['_launch_match'] = str(launch.get('mission', '') or '')
        corridor = _build_straight_line_corridor_polygon(raw)
        if corridor:
            notam['polygon'] = corridor
            notam['latitude'] = round(sum(point[0] for point in corridor) / len(corridor), 6)
            notam['longitude'] = round(sum(point[1] for point in corridor) / len(corridor), 6)
            notam['radius'] = ''
        return notam['_launch_match']
    return ''

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
    if any(d in raw_upper for d in DROP):
        return False
    # UNL is an altitude, while QRDCA/QWMLW/QWELW are generic danger/activity
    # codes.  None is sufficient evidence of a launch by itself.  Accept only
    # explicit aerospace-event language or an upstream source that already
    # applied a launch-specific classifier.
    trusted_source = bool(notam.get('_trusted_launch_source'))
    if not trusted_source and not any(pattern.search(raw_upper) for pattern in KEEP_PATTERNS):
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

def _parse_supplement_polygon(value):
    """Parse Joey0609 compact polygon strings (DDMM/DMS + hemisphere prefix)."""
    points = []
    pattern = re.compile(r'([NS])(\d{4,6})([EW])(\d{5,7})', re.I)
    for match in pattern.finditer(str(value or '')):
        lat_h, lat_digits, lon_h, lon_digits = match.groups()
        lat = _parse_coord_val(lat_digits, lat_h)
        lon = _parse_coord_val(lon_digits, lon_h)
        points.append([round(lat, 6), round(lon, 6)])
    if len(points) > 1 and points[0] == points[-1]:
        points.pop()
    return points if len(points) >= 3 else []

def _supplement_country_from_fir(fir):
    fir = str(fir or '').strip().upper()
    if fir == 'RPHI':
        return 'PHILIPPINES'
    if fir in {'RJJJ', 'RJTG'}:
        return 'JAPAN'
    if fir.startswith('Z'):
        return 'CHINA'
    return 'GLOBAL'

def _iter_global_supplement_features(data):
    """Yield a common feature shape for legacy GeoJSON and current data_dict.json."""
    if not isinstance(data, dict):
        return

    legacy = data.get('features')
    if isinstance(legacy, list):
        for feat in legacy:
            if isinstance(feat, dict):
                item = dict(feat)
                item['_supplement_schema'] = 'legacy_geojson'
                yield item
        return

    section = data.get('NOTAM_DATA')
    if not isinstance(section, dict) or not isinstance(section.get('CODE'), list):
        section = data
    codes = section.get('CODE', [])
    raws = section.get('RAWMESSAGE', [])
    coordinates = section.get('COORDINATES', [])
    platform_ids = section.get('PLATID', [])
    firs = section.get('FIR', [])
    sources = section.get('SOURCE', [])
    size = min(len(codes), len(raws))
    for index in range(size):
        source = str(sources[index] if index < len(sources) else 'NOTAM').strip().upper()
        if source and source != 'NOTAM':
            continue
        fir = str(firs[index] if index < len(firs) else '').strip().upper()
        code = str(codes[index] or '').strip().upper()
        platform_id = str(platform_ids[index] if index < len(platform_ids) else '').strip()
        coordinate_text = coordinates[index] if index < len(coordinates) else ''
        yield {
            '_supplement_schema': 'joey_data_dict',
            'id': f"joey-{platform_id or code}",
            'name': code,
            'description': str(raws[index] or ''),
            'fir': fir,
            'country': _supplement_country_from_fir(fir),
            'polygon': _parse_supplement_polygon(coordinate_text),
        }

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

def _parse_compact_faa_coord(lat_digits, lat_hemi, lon_digits, lon_hemi):
    return (
        _parse_coord_val(lat_digits, lat_hemi),
        _parse_coord_val(lon_digits, lon_hemi),
    )

def _faa_bearing_deg(origin, point):
    lat1, lon1 = map(math.radians, origin)
    lat2, lon2 = map(math.radians, point)
    y = math.sin(lon2 - lon1) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0

def _faa_distance_nm(origin, point):
    lat1, lon1 = map(math.radians, origin)
    lat2, lon2 = map(math.radians, point)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 3440.065 * 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))

def _faa_destination_point(origin, bearing_deg, distance_nm):
    lat1, lon1 = map(math.radians, origin)
    bearing = math.radians(bearing_deg)
    angular = distance_nm / 3440.065
    lat2 = math.asin(
        math.sin(lat1) * math.cos(angular)
        + math.cos(lat1) * math.sin(angular) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(angular) * math.cos(lat1),
        math.cos(angular) - math.sin(lat1) * math.sin(lat2),
    )
    lon2 = (lon2 + math.pi) % (2 * math.pi) - math.pi
    return [round(math.degrees(lat2), 6), round(math.degrees(lon2), 6)]

def _interpolate_faa_tfr_arc(start, end, center, direction, stated_radius_nm):
    """Expand an FAA clockwise/counterclockwise boundary arc into map points."""
    start_bearing = _faa_bearing_deg(center, start)
    end_bearing = _faa_bearing_deg(center, end)
    clockwise = str(direction).strip().lower().startswith('clockwise')
    sweep = (end_bearing - start_bearing) % 360.0 if clockwise else -((start_bearing - end_bearing) % 360.0)
    steps = max(2, int(math.ceil(abs(sweep) / 5.0)))
    start_radius = _faa_distance_nm(center, start) or float(stated_radius_nm)
    end_radius = _faa_distance_nm(center, end) or float(stated_radius_nm)
    points = []
    for index in range(1, steps + 1):
        fraction = index / steps
        if index == steps:
            points.append([round(end[0], 6), round(end[1], 6)])
            continue
        bearing = start_bearing + sweep * fraction
        radius = start_radius + (end_radius - start_radius) * fraction
        points.append(_faa_destination_point(center, bearing, radius))
    return points

def _extract_faa_tfr_polygon(normalized_text):
    """Parse FAA Region-bounded text, preserving ARC semantics instead of treating centers as vertices."""
    area_match = re.search(r'Region bounded by:(.*?)(?:Altitude:|Effective Date)', normalized_text, re.I | re.S)
    if not area_match:
        return []
    area = area_match.group(1)
    coord_pattern = r'(\d{6})([NS])\s+(\d{7})([EW])'
    arc_pattern = re.compile(
        rf'(Clockwise|Counterclockwise)\s+on\s+a\s+(\d+(?:\.\d+)?)\s*NM\s+ARC\s+Centered\s+on:\s*'
        rf'{coord_pattern}.*?\bTo:\s*{coord_pattern}',
        re.I | re.S,
    )
    arc = arc_pattern.search(area)

    def coords_from(text):
        return [
            list(_parse_compact_faa_coord(*match.groups()))
            for match in re.finditer(coord_pattern, text, re.I)
        ]

    if not arc:
        points = coords_from(area)
        return points if len(points) >= 3 else []

    prefix = coords_from(area[:arc.start()])
    if not prefix:
        return []
    groups = arc.groups()
    direction, radius = groups[0], float(groups[1])
    center = _parse_compact_faa_coord(groups[2], groups[3], groups[4], groups[5])
    end = _parse_compact_faa_coord(groups[6], groups[7], groups[8], groups[9])
    suffix = coords_from(area[arc.end():])
    polygon = prefix + _interpolate_faa_tfr_arc(prefix[-1], end, center, direction, radius) + suffix
    return polygon if len(polygon) >= 3 else []

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
    polygon = _extract_faa_tfr_polygon(normalized_text)
    if not _is_in_time_window(from_utc, to_utc):
        return None

    notam_id = str(summary.get('notam_id', '') or records[0].get('notam_id', '')).strip()
    facility = str(summary.get('facility', '') or '').strip().upper()
    description = str(summary.get('description', '') or '').strip()
    raw = (
        f"FDC {notam_id} AIRSPACE SPACE OPERATIONS TFR. {description}. "
        f"{normalized_text}"
    ).strip()
    centroid_points = polygon or points
    lat = round(sum(p[0] for p in centroid_points) / len(centroid_points), 6) if centroid_points else ''
    lon = round(sum(p[1] for p in centroid_points) / len(centroid_points), 6) if centroid_points else ''
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
            'polygon': polygon,
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

    for feat in _iter_global_supplement_features(data):
        if (
            feat.get('_supplement_schema') == 'legacy_geojson'
            and feat.get('eventType') not in GLOBAL_SUPPLEMENT_TYPES
        ):
            continue
        raw = str(feat.get('description', '') or '')
        if not raw:
            continue
        notam_id = _extract_notam_id(raw, feat.get('name', ''))
        q_m = re.search(r'Q\)\s*([A-Z0-9]{4})/([A-Z0-9]{5})/', raw)
        fir = q_m.group(1).strip() if q_m else str(feat.get('fir', feat.get('site', '')) or '')
        qcode = q_m.group(2).strip() if q_m else ''
        lat, lon, radius, parsed_qcode = _parse_q_line(raw)
        if not qcode:
            qcode = parsed_qcode
        polygon = feat.get('polygon', [])
        if polygon:
            lat = round(sum(point[0] for point in polygon) / len(polygon), 6)
            lon = round(sum(point[1] for point in polygon) / len(polygon), 6)
            radius = ''
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
            'notam_id': notam_id,
            'polygon': polygon,
            '_trusted_launch_source': True,
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

def fetch_faa_notams(launch_contexts=()):
    rows = []
    headers = {
        **make_headers(),
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://notams.aim.faa.gov",
        "Referer": "https://notams.aim.faa.gov/notamSearch/nsapp.html",
    }
    active_launch_firs = set()
    for launch in launch_contexts or []:
        active_launch_firs.update(LAUNCH_SITE_FIRS.get(str(launch.get('site', '')).casefold(), set()))

    for fir in FAA_SUPPLEMENTAL_FIRS:
        session = requests.Session()
        session.headers.update(headers)
        offset = 0
        pages_fetched = 0
        page_limit = (
            FAA_MAX_PAGES_FOR_ACTIVE_LAUNCH_FIR
            if fir in active_launch_firs
            else FAA_MAX_PAGES_PER_FIR
        )
        while pages_fetched < page_limit:
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
                    launch_match = _correlate_silent_launch_notam(n, launch_contexts)
                    if launch_match:
                        print(f"[faa] Correlated {notam_num} with upcoming launch: {launch_match}")
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

def _same_coordinate(a, b, tolerance=1e-6):
    return abs(a[0] - b[0]) <= tolerance and abs(a[1] - b[1]) <= tolerance

def _split_coordinate_rings(indexed_points, max_gap=150):
    """Split raw NOTAM points at explicit ring closures or large text gaps."""
    rings = []
    current = []
    last_index = None
    previous_point = None

    for point, index in indexed_points:
        # Parenthesized NOTAM closures often repeat the same closing point
        # twice. Ignore only adjacent duplicates; a repeated first point is
        # still needed to detect the end of a ring.
        if previous_point is not None and _same_coordinate(previous_point, point):
            last_index = index
            continue

        if current and last_index is not None and index - last_index >= max_gap:
            if len(current) >= 3:
                rings.append(current)
            current = []

        current.append(point)
        previous_point = point
        last_index = index

        if len(current) >= 4 and _same_coordinate(current[0], current[-1]):
            ring = current[:-1]
            if len(ring) >= 3:
                rings.append(ring)
            current = []
            last_index = None

    if len(current) >= 3:
        rings.append(current)
    return rings

def _extract_raw_coordinate_rings(raw):
    """Extract one or more polygon rings from a raw ICAO NOTAM message."""
    coord_regex = re.compile(
        r'(?:([NS])\s*(\d{4,6}(?:[.,]\d+)?))\s*'
        r'(?:([EW])\s*(\d{5,7}(?:[.,]\d+)?))|'
        r'(?:(\d{4,6}(?:[.,]\d+)?)\s*([NS]))\s*'
        r'(?:(\d{5,7}(?:[.,]\d+)?)\s*([EW]))',
        re.I,
    )
    cleaned = re.sub(r'Q\).*?(?=\s*A\))', '', str(raw or ''), flags=re.DOTALL)
    indexed_points = []
    for match in coord_regex.finditer(cleaned):
        if match.group(1):
            lat = _parse_coord_val(match.group(2), match.group(1))
            lon = _parse_coord_val(match.group(4), match.group(3))
        else:
            lat = _parse_coord_val(match.group(5), match.group(6))
            lon = _parse_coord_val(match.group(7), match.group(8))
        indexed_points.append(([lat, lon], match.start()))
    return _split_coordinate_rings(indexed_points)

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
        polygon_raw = row.get('polygon', '').strip()
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

        explicit_polygons = []
        if polygon_raw:
            try:
                parsed = json.loads(polygon_raw)
                if isinstance(parsed, list) and len(parsed) >= 3 and isinstance(parsed[0], list):
                    if parsed and parsed[0] and isinstance(parsed[0][0], (int, float)):
                        explicit_polygons = [parsed]
                    elif parsed and parsed[0] and isinstance(parsed[0][0], list):
                        explicit_polygons = [ring for ring in parsed if len(ring) >= 3]
            except (ValueError, TypeError):
                explicit_polygons = []

        raw_polygons = _extract_raw_coordinate_rings(raw)
        polygons = explicit_polygons or raw_polygons

        if polygons:
            ET.SubElement(pm, 'styleUrl').text = '#polyStyle'
            geometry_parent = pm if len(polygons) == 1 else ET.SubElement(pm, 'MultiGeometry')
            for points in polygons:
                poly = ET.SubElement(geometry_parent, 'Polygon')
                outer = ET.SubElement(poly, 'outerBoundaryIs')
                ring = ET.SubElement(outer, 'LinearRing')
                coords_text = [f"{float(point[1])},{float(point[0])},0" for point in points]
                if coords_text and coords_text[-1] != coords_text[0]:
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

    launch_contexts = fetch_upcoming_launch_context()
    items = fetch_notammap()
    supplemental = fetch_faa_notams(launch_contexts)
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
            polygon = n.get('polygon', '')
            if radius == 999:
                radius = ''
            from_utc = n.get('from', '')
            to_utc = n.get('to', '')
            writer.writerow([
                country, str(nid), notam_id, fir, from_utc, to_utc,
                str(lat) if lat != '' else '',
                str(lon) if lon != '' else '',
                str(radius) if radius != '' else '',
                qcode, raw,
                json.dumps(polygon, ensure_ascii=False, separators=(',', ':')) if polygon else ''
            ])

    print(f"\nPipeline complete: {len(items)} records written to notams.csv")

    csv_to_kml('notams.csv', 'notams.kml')
    archive_weekly('notams.csv', 'notams')
    print("=" * 60)

if __name__ == '__main__':
    main()

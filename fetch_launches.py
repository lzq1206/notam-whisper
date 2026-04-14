import requests
import re
import csv
from datetime import datetime
import os

CSV_HEADERS = [
    "Launch Date and Time (UTC)",
    "Launch Site (Abbrv.)",
    "Latitude",
    "Longitude",
    "Launch Vehicle",
    "Official Payload Name",
    "Success",
    "Launch Site (Full)",
]

LAUNCH_SITE_ALIASES = {
    "jiuquan": "JSLC",
    "taiyuan": "TSLC",
    "xichang": "XSLC",
    "wenchang": "WSLC",
    "hainan commercial": "HCSLS",
    "haiyang": "YSLA",
    "yangjiang": "SSLA",
    "rizhao": "RSLA",
    "kennedy": "KSC",
    "cape canaveral": "CCSFS",
    "vandenberg": "VSFB",
    "boca chica": "Starbase",
    "starbase": "Starbase",
    "wallops": "WFF",
    "kodiak": "PSCA",
    "baikonur": "Baikonur",
    "kourou": "CSG",
    "guiana space centre": "CSG",
    "tanegashima": "TNSC",
    "satish dhawan": "SDSC",
    "naro": "Naro",
    "rocket lab launch complex 1": "LC-1",
    "new zealand": "LC-1",
}


def _normalize_launch_datetime(date_text):
    text = (date_text or "").strip().upper()
    if not text:
        return ""

    # Try common formats first (with year).
    for fmt in ("%b %d, %Y", "%b %d %Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(text.title(), fmt)
            return dt.strftime("%Y %b %d 0000").upper()
        except ValueError:
            pass

    month_day_match = re.search(
        r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{1,2})\b", text
    )
    if month_day_match:
        month, day = month_day_match.groups()
        year_match = re.search(r"\b(20\d{2})\b", text)
        year = year_match.group(1) if year_match else str(datetime.now().year)
        return f"{year} {month} {int(day):02d} 0000"

    return ""


def _launch_sort_key(item):
    launch_date = item.get("Launch Date and Time (UTC)", "")
    try:
        return datetime.strptime(launch_date, "%Y %b %d %H%M")
    except Exception:
        return datetime.min


def _load_launch_sites():
    launch_sites_path = os.path.join(os.path.dirname(__file__), "launch_sites.csv")
    sites = []
    with open(launch_sites_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = (row.get("name") or "").strip()
            abbr = (row.get("abbreviation") or "").strip()
            if not name:
                continue
            latitude = row.get("latitude", "")
            longitude = row.get("longitude", "")
            sites.append(
                {
                    "name": name,
                    "abbr": abbr,
                    "latitude": float(latitude) if latitude not in ("", None) else "",
                    "longitude": float(longitude) if longitude not in ("", None) else "",
                }
            )
    return sites


def _resolve_site(location, launch_sites, site_by_abbr):
    normalized = (location or "").lower()
    if not normalized:
        return "", "", ""

    for key, abbr in LAUNCH_SITE_ALIASES.items():
        if key in normalized:
            site = site_by_abbr.get(abbr.lower())
            if site:
                return site["latitude"], site["longitude"], site["abbr"]

    candidates = []
    for site in launch_sites:
        name_l = site["name"].lower()
        abbr_l = site["abbr"].lower()
        if name_l and name_l in normalized:
            candidates.append((len(name_l), site))
        elif abbr_l and abbr_l in normalized:
            candidates.append((len(abbr_l), site))

    if not candidates:
        return "", "", ""

    _score, best_site = max(candidates, key=lambda x: x[0])
    return best_site["latitude"], best_site["longitude"], best_site["abbr"]


def _extract_rll_api_results(payload):
    if isinstance(payload, dict):
        for key in ("result", "results", "launches", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    if isinstance(payload, list):
        return payload
    return []


def _parse_rll_api_result(payload, launch_sites, site_by_abbr):
    launches = []
    for item in _extract_rll_api_results(payload):
        if not isinstance(item, dict):
            continue

        date_raw = item.get("date_str") or item.get("date") or item.get("win_open")
        mission = (item.get("name") or item.get("mission_name") or item.get("slug") or "").strip()
        vehicle = (
            item.get("vehicle_name")
            or item.get("vehicle")
            or item.get("rocket")
            or item.get("provider")
            or ""
        ).strip()

        location_raw = ""
        pad = item.get("pad")
        if isinstance(pad, dict):
            location_raw = (
                pad.get("name")
                or pad.get("location")
                or pad.get("locality")
                or pad.get("statename")
                or ""
            ).strip()
            if not vehicle:
                vehicle = (pad.get("vehicle_name") or "").strip()
        if not location_raw:
            location_raw = (
                item.get("location")
                or item.get("pad_name")
                or item.get("launch_site")
                or item.get("state")
                or ""
            ).strip()

        full_date = _normalize_launch_datetime(date_raw)
        if not full_date:
            continue

        lat, lon, abbr = _resolve_site(location_raw, launch_sites, site_by_abbr)
        success_raw = item.get("launch_success")
        if success_raw is None:
            success_raw = item.get("result") or item.get("status")

        success = "S"
        if isinstance(success_raw, bool):
            success = "S" if success_raw else "F"
        elif isinstance(success_raw, str):
            lowered = success_raw.strip().lower()
            if lowered in ("false", "fail", "failed", "failure", "unsuccessful"):
                success = "F"
            elif lowered in ("true", "success", "successful", "succeeded"):
                success = "S"

        launches.append({
            "Launch Date and Time (UTC)": full_date,
            "Launch Site (Abbrv.)": abbr,
            "Latitude": lat,
            "Longitude": lon,
            "Launch Vehicle": vehicle,
            "Official Payload Name": mission,
            "Success": success,
            "Launch Site (Full)": location_raw,
        })
    return launches


def fetch_past_launches():
    launch_sites = _load_launch_sites()
    site_by_abbr = {site["abbr"].lower(): site for site in launch_sites if site["abbr"]}

    url = "https://www.rocketlaunch.live/?pastOnly=1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return []
        
        html = response.text
        launches = []
        month_abbrs = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
        date_pattern = rf"{month_abbrs}\s+\d+"
        
        # More specific regex for the RLL past launches page structure
        items = re.findall(rf'<span class="launch-date">({date_pattern}).*?<h4 class="mission-name">.*?>(.*?)<.*?vehicle-name-inner">\s*(.*?)\s*<.*?location.*?>(.*?)<', html, re.S)
        
        if not items:
            # Fallback regex if inner tags differ
            items = re.findall(rf'({date_pattern}).*?mission-name.*?>(.*?)<.*?vehicle.*?>(.*?)<.*?location.*?>(.*?)<', html, re.S)

        for item in items:
            if len(item) != 4:
                continue
            date_str, mission_raw, vehicle_raw, location_raw = item
            mission = mission_raw.strip()
            vehicle = vehicle_raw.strip()
            location = location_raw.strip()
            full_date = _normalize_launch_datetime(date_str)
            if not full_date:
                continue
            
            lat, lon, abbr = _resolve_site(location, launch_sites, site_by_abbr)

            launches.append({
                "Launch Date and Time (UTC)": full_date,
                "Launch Site (Abbrv.)": abbr,
                "Latitude": lat,
                "Longitude": lon,
                "Launch Vehicle": vehicle,
                "Official Payload Name": mission,
                "Success": "S",
                "Launch Site (Full)": location
            })

        # RocketLaunch.Live page structure can change; use API fallback when HTML regex finds nothing.
        if not launches:
            for api_url in (
                "https://fdo.rocketlaunch.live/json/launches/past/100",
                "https://fdo.rocketlaunch.live/json/launches/past/50",
            ):
                try:
                    api_response = requests.get(api_url, headers=headers, timeout=30)
                    if api_response.status_code != 200:
                        continue
                    api_launches = _parse_rll_api_result(api_response.json(), launch_sites, site_by_abbr)
                    if api_launches:
                        launches = api_launches
                        break
                except Exception:
                    continue

        return launches
    except Exception as e:
        print(f"Error scraping: {e}")
        return []

def save_to_csv(launches, filename):
    existing_data = []
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_data = list(reader)
    
    # Merge and Deduplicate (based on Date + Vehicle + Payload)
    combined = existing_data + launches
    unique = {}
    for item in combined:
        # Create a unique key
        k = f"{item.get('Launch Date and Time (UTC)')}|{item.get('Launch Vehicle')}|{item.get('Official Payload Name')}"
        if k not in unique:
            unique[k] = item
    
    # Sort by date (descending)
    final_list = sorted(unique.values(), key=_launch_sort_key, reverse=True)

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(final_list)


def archive_weekly(csv_path, history_subdir='launches'):
    history_dir = os.path.join('history', history_subdir)
    os.makedirs(history_dir, exist_ok=True)

    today = datetime.utcnow().date()
    iso_year, iso_week, _ = today.isocalendar()
    week_tag = f"{iso_year}-W{iso_week:02d}"
    weekly_csv = os.path.join(history_dir, f"{week_tag}.csv")

    new_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or CSV_HEADERS
        new_rows = list(reader)

    existing_rows = []
    if os.path.exists(weekly_csv):
        with open(weekly_csv, 'r', encoding='utf-8') as f:
            existing_rows = list(csv.DictReader(f))

    seen = set()
    merged = []
    for row in new_rows + existing_rows:
        key = f"{row.get('Launch Date and Time (UTC)')}|{row.get('Launch Vehicle')}|{row.get('Official Payload Name')}"
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    merged = sorted(merged, key=_launch_sort_key, reverse=True)
    with open(weekly_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(merged)


if __name__ == "__main__":
    launches = fetch_past_launches()
    if launches:
        # Only save if we actually got something from the scrape
        csv_path = os.path.join(os.path.dirname(__file__), "past_launches.csv")
        save_to_csv(launches, csv_path)
        archive_weekly(csv_path, "launches")
        print(f"Merged {len(launches)} new/recent launches into past_launches.csv")
    else:
        print("No new launches found or scrape failed.")

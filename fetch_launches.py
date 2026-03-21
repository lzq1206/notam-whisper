import requests
import re
import csv
from datetime import datetime
import os

def fetch_past_launches():
    # Mapping for scraped location names to coordinates/abbreviations
    SITE_MAPPING = {
        "California": {"lat": 34.742, "lon": -120.572, "abbr": "VSFB"},
        "Florida": {"lat": 28.572, "lon": -80.648, "abbr": "KSC"},
        "New Zealand": {"lat": -39.261, "lon": 177.864, "abbr": "MP"},
        "French Guiana": {"lat": 5.232, "lon": -52.776, "abbr": "CSG"},
        "Kazakhstan": {"lat": 45.965, "lon": 63.305, "abbr": "BC"},
        "China": {"lat": 28.246, "lon": 102.026, "abbr": "XSLC"},
        "India": {"lat": 13.720, "lon": 80.231, "abbr": "SDSC"},
        "Japan": {"lat": 30.375, "lon": 130.955, "abbr": "TNSC"},
        "South Korea": {"lat": 34.432, "lon": 127.535, "abbr": "NSC"},
        "Wallops": {"lat": 37.833, "lon": -75.483, "abbr": "WFF"},
        "Kodiak": {"lat": 57.435, "lon": -152.339, "abbr": "PSCA"},
    }

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
        date_pattern = rf"({month_abbrs})\s+(\d+)"
        
        # More specific regex for the RLL past launches page structure
        items = re.findall(rf'<span class="launch-date">({date_pattern}).*?<h4 class="mission-name">.*?>(.*?)<.*?vehicle-name-inner">\s*(.*?)\s*<.*?location.*?>(.*?)<', html, re.S)
        
        if not items:
            # Fallback regex if inner tags differ
            items = re.findall(rf'({date_pattern}).*?mission-name.*?>(.*?)<.*?vehicle.*?>(.*?)<.*?location.*?>(.*?)<', html, re.S)

        for item in items:
            date_str = item[0]
            mission = item[2].strip()
            vehicle = item[3].strip()
            location = item[4].strip()
            
            dt = datetime.now()
            full_date = f"{dt.year} {date_str} 0000"
            
            # Map location
            lat, lon, abbr = "", "", ""
            for key, val in SITE_MAPPING.items():
                if key.lower() in location.lower():
                    lat, lon, abbr = val["lat"], val["lon"], val["abbr"]
                    break

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
            
        return launches
    except Exception as e:
        print(f"Error scraping: {e}")
        return []

def save_to_csv(launches, filename):
    keys = ["Launch Date and Time (UTC)", "Launch Site (Abbrv.)", "Latitude", "Longitude", "Launch Vehicle", "Official Payload Name", "Success", "Launch Site (Full)"]
    
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
    final_list = sorted(unique.values(), key=lambda x: x.get('Launch Date and Time (UTC)', ''), reverse=True)

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(final_list)

if __name__ == "__main__":
    launches = fetch_past_launches()
    if launches:
        # Only save if we actually got something from the scrape
        save_to_csv(launches, os.path.join(os.path.dirname(__file__), "past_launches.csv"))
        print(f"Merged {len(launches)} new/recent launches into past_launches.csv")
    else:
        print("No new launches found or scrape failed.")

import requests
import json
import re
import datetime
from datetime import timedelta
import csv

KEEP = ["UNL", "AEROSPACE", "RE-ENTRY", "ROCKET", "AEROSPACE"]
DROP = [
    "KWAJALEIN","BALLOON","BALLON","TRANSMITTER","GUNFIRING","AERIAL","GUN FRNG",
    "AIR EXER","REF AIP","MISSILES","KOLKATA","MWARA","ZS(D)","ZY(R)","ZG(R)",
    "SHIQUANHE","MEDEVAC","WOOMERA AIRSPACE","MAVLA","VED-52 ACT",
    "LASER DANGER AREA","ACFT MANEUVERING","ATTENTION ACFT","STNR ALT RESERVATION",
    "UNTIL PERM","MILITARY FLIGHTS","DUE MIL FLYING","EMERALD","SATPHONE",
    "OAKLAND ATC","UNLIT","UNLESS","ADS-B","UNLOAD","3000FT","UNMANNED ACFT", "VVIP MOV",
    "FL200","400FT AGL","49215FT AMSL","1350FT AMSL","9000FT AMSL",
    "QUEENSLAND","LASER DISPLAY","UNLIGHTED", "6-87 ROCKET", "6-86 ROCKET", "6-89 ROCKET",
    "RADIOSONDE", "MODEL ROCKET","VOLCAN",
]

SEARCH_TERMS = ["AEROSPACE", "RE-ENTRY", "ROCKET", "SPACE DEBRIS", "DNG ZONE"]
ICAO_LIST = [
    "ZBPE", "ZGZU", "ZHWH", "ZJSA", "ZLHW", "ZPKM", "ZSHA", "ZWUQ", "ZYSH", "VHHK"
]

def make_headers():
    return {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

def fetch_notams():
    session = requests.Session()
    session.headers.update(make_headers())
    url = "https://notams.aim.faa.gov/notamSearch/search"
    results = []

    print("Fetching global Free Text keywords...")
    for term in SEARCH_TERMS:
        payload = {
            "searchType": "4",
            "offset": "0",
            "freeFormText": term,
            "notamsOnly": "false"
        }
        for page in range(10):
            payload["offset"] = str(page * 30)
            try:
                resp = session.post(url, data=payload, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    notams = data.get('notamList', [])
                    results.extend(notams)
                    if len(notams) < 30:
                        break
            except Exception as e:
                print(f"Error fetching term '{term}': {e}")
                break

    print("Fetching specific FIR lists...")
    for icao in ICAO_LIST:
        payload = {
            "searchType": "0",
            "designatorsForLocation": icao,
            "offset": "0",
            "notamsOnly": "false"
        }
        for page in range(10):
            payload["offset"] = str(page * 30)
            try:
                resp = session.post(url, data=payload, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    notams = data.get('notamList', [])
                    results.extend(notams)
                    if len(notams) < 30:
                        break
            except Exception as e:
                print(f"Error fetching FIR '{icao}': {e}")
                break
                
    return results

def parse_date(date_str):
    if not date_str: return None
    try:
        return datetime.datetime.strptime(date_str, "%m/%d/%Y %H%M")
    except:
        return None

def main():
    notams = fetch_notams()
    unique_notams = {}
    for n in notams:
        if 'notamNumber' in n:
            unique_notams[n['notamNumber']] = n
            
    filtered = []
    now = datetime.datetime.utcnow()
    five_days_later = now + timedelta(days=5)
    
    for n_id, n in unique_notams.items():
        msg = n.get('icaoMessage', '').upper()
        
        # Apply DROP filter
        if any(d in msg for d in DROP):
            continue
            
        # Apply KEEP filter
        if not any(k in msg for k in KEEP):
            continue
        
        start_date = parse_date(n.get('startDate'))
        end_date = parse_date(n.get('endDate'))
        
        if start_date and start_date > five_days_later:
            continue
        if end_date and end_date < now:
            continue
            
        filtered.append(n)
        
    print(f"Dataset refined to {len(filtered)} valid NOTAMs.")
    
    with open('latest.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['country','id','notam_id','fir','from_utc','to_utc','lat','lon','radius_nm','qcode','raw'])
        
        for n in filtered:
            msg = n.get('icaoMessage', '')
            notam_id = n.get('notamNumber', '')
            fir = n.get('facilityDesignator', '')
            
            from_utc, to_utc = '', ''
            sd = parse_date(n.get('startDate'))
            if sd: from_utc = sd.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            ed = parse_date(n.get('endDate'))
            if ed: to_utc = ed.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            
            lat, lon, rad, qcode = '', '', '', ''
            
            q_match = re.search(r'Q\)\s*([A-Z]{4})/([A-Z]{5})/[^/]*/[^/]*/[^/]*/\d{3}/\d{3}/(\d{4}[NS]\d{5}[EW]|\d{6}[NS]\d{7}[EW])(\d{3})', msg)
            if q_match:
                fir = q_match.group(1)
                qcode = q_match.group(2)
                coord_str = q_match.group(3)
                rad = str(int(q_match.group(4)))
                
                try:
                    lat_partObj = re.match(r'(\d+)([NS])(.*)', coord_str)
                    if lat_partObj:
                        lat_raw, lat_hemi, lon_raw_rest = lat_partObj.groups()
                        lon_partObj = re.match(r'(\d+)([EW])', lon_raw_rest)
                        if lon_partObj:
                            lon_raw, lon_hemi = lon_partObj.groups()
                            
                            lat_val = int(lat_raw[:2]) + int(lat_raw[2:4])/60.0 + (int(lat_raw[4:6])/3600.0 if len(lat_raw)==6 else 0)
                            if lat_hemi == 'S': lat_val = -lat_val
                            
                            lon_val = int(lon_raw[:3]) + int(lon_raw[3:5])/60.0 + (int(lon_raw[5:7])/3600.0 if len(lon_raw)==7 else 0)
                            if lon_hemi == 'W': lon_val = -lon_val
                            
                            lat, lon = str(round(lat_val, 6)), str(round(lon_val, 6))
                except Exception as e:
                    print(f"Error parsing coord: {coord_str}", e)
                    
            writer.writerow(['Global', n.get('transactionID', ''), notam_id, fir, from_utc, to_utc, lat, lon, rad, qcode, msg])
            
if __name__ == '__main__':
    main()

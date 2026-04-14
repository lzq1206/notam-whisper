#!/usr/bin/env python3
import csv
import os
import tempfile

from fetch_launches import _load_launch_sites, _parse_rll_api_result, _resolve_site, archive_weekly, save_to_csv


def _read_rows(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_archive_weekly_creates_history_and_dedupes():
    sample = [
        {
            "Launch Date and Time (UTC)": "2026 MAR 20 0000",
            "Launch Site (Abbrv.)": "VSFB",
            "Latitude": "34.742",
            "Longitude": "-120.572",
            "Launch Vehicle": "Falcon 9",
            "Official Payload Name": "Starlink-371",
            "Success": "S",
            "Launch Site (Full)": "California, United States",
        },
        {
            "Launch Date and Time (UTC)": "2026 MAR 20 0000",
            "Launch Site (Abbrv.)": "VSFB",
            "Latitude": "34.742",
            "Longitude": "-120.572",
            "Launch Vehicle": "Falcon 9",
            "Official Payload Name": "Starlink-371",
            "Success": "S",
            "Launch Site (Full)": "California, United States",
        },
        {
            "Launch Date and Time (UTC)": "2026 MAR 19 0000",
            "Launch Site (Abbrv.)": "KSC",
            "Latitude": "28.572",
            "Longitude": "-80.648",
            "Launch Vehicle": "Falcon 9",
            "Official Payload Name": "Starlink-370",
            "Success": "S",
            "Launch Site (Full)": "Florida, United States",
        },
    ]

    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        csv_path = os.path.join(tmpdir, "past_launches.csv")
        save_to_csv(sample, csv_path)
        archive_weekly(csv_path, "launches")
        archive_weekly(csv_path, "launches")

        history_dir = os.path.join(tmpdir, "history", "launches")
        files = [f for f in os.listdir(history_dir) if f.endswith(".csv")]
        assert len(files) == 1
        weekly_csv = os.path.join(history_dir, files[0])
        rows = _read_rows(weekly_csv)
        assert len(rows) == 2
        assert rows[0]["Launch Date and Time (UTC)"] == "2026 MAR 20 0000"
        assert rows[1]["Launch Date and Time (UTC)"] == "2026 MAR 19 0000"
    os.chdir(cwd)


def test_resolve_site_matches_specific_launch_site():
    launch_sites = _load_launch_sites()
    site_by_abbr = {site["abbr"].lower(): site for site in launch_sites if site["abbr"]}

    lat, lon, abbr = _resolve_site("Wenchang, Hainan, China", launch_sites, site_by_abbr)
    assert abbr == "WSLC"
    assert lat == 19.614
    assert lon == 110.951

    lat, lon, abbr = _resolve_site("Cape Canaveral, Florida, United States", launch_sites, site_by_abbr)
    assert abbr == "CCSFS"
    assert lat == 28.488
    assert lon == -80.577


def test_parse_rll_api_result_maps_fields():
    launch_sites = _load_launch_sites()
    site_by_abbr = {site["abbr"].lower(): site for site in launch_sites if site["abbr"]}
    payload = {
        "result": [
            {
                "date_str": "MAR 12",
                "name": "Starlink Group",
                "vehicle_name": "Falcon 9",
                "pad": {"location": "Cape Canaveral, Florida, United States"},
            }
        ]
    }

    launches = _parse_rll_api_result(payload, launch_sites, site_by_abbr)
    assert len(launches) == 1
    assert launches[0]["Launch Site (Abbrv.)"] == "CCSFS"
    assert launches[0]["Launch Vehicle"] == "Falcon 9"
    assert launches[0]["Official Payload Name"] == "Starlink Group"


if __name__ == "__main__":
    test_archive_weekly_creates_history_and_dedupes()
    test_resolve_site_matches_specific_launch_site()
    test_parse_rll_api_result_maps_fields()
    print("test_fetch_launches_history.py passed")

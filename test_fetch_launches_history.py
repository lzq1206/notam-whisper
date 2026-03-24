#!/usr/bin/env python3
import csv
import os
import tempfile

import fetch_launches
from fetch_launches import archive_weekly, fetch_past_launches, save_to_csv


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


if __name__ == "__main__":
    test_archive_weekly_creates_history_and_dedupes()
    def test_fetch_past_launches_falls_back_to_api_when_html_layout_changes():
        html_no_match = "<html><body>layout changed</body></html>"
        api_payload = {
            "result": [
                {
                    "name": "Starlink Test Mission",
                    "win_open": "2026-03-24T05:30:00Z",
                    "vehicle": {"name": "Falcon 9"},
                    "pad": {"location": {"name": "California", "latitude": 34.742, "longitude": -120.572}},
                }
            ]
        }

        class StubResponse:
            def __init__(self, status_code=200, text="", payload=None):
                self.status_code = status_code
                self.text = text
                self._payload = payload

            def json(self):
                return self._payload

        old_get = fetch_launches.requests.get
        try:
            calls = []

            def stub_get(url, *args, **kwargs):
                calls.append(url)
                if "pastOnly=1" in url:
                    return StubResponse(200, text=html_no_match)
                if "/json/launches/past/" in url:
                    return StubResponse(200, payload=api_payload)
                return StubResponse(404, text="")

            fetch_launches.requests.get = stub_get
            rows = fetch_past_launches()
            assert len(rows) == 1
            row = rows[0]
            assert row["Official Payload Name"] == "Starlink Test Mission"
            assert row["Launch Vehicle"] == "Falcon 9"
            assert row["Launch Date and Time (UTC)"] == "2026 MAR 24 0530"
            assert row["Launch Site (Abbrv.)"] == "VSFB"
            assert any("/json/launches/past/" in u for u in calls), f"API fallback not called: {calls}"
        finally:
            fetch_launches.requests.get = old_get

    test_fetch_past_launches_falls_back_to_api_when_html_layout_changes()
    print("test_fetch_launches_history.py passed")

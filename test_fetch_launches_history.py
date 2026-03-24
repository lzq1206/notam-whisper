#!/usr/bin/env python3
import csv
import os
import tempfile

from fetch_launches import archive_weekly, save_to_csv


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
    print("test_fetch_launches_history.py passed")

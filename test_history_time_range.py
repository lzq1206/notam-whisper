#!/usr/bin/env python3
import os
import csv
import tempfile
import datetime

import fetch_notams
import fetch_msi


def _write_csv(path, headers, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def test_notams_archive_removes_out_of_range():
    now = datetime.datetime(2026, 3, 24, 2, 0, 0)
    fetch_notams.now_utc = now
    fetch_notams.five_days = now + datetime.timedelta(days=5)
    fetch_notams.csv_to_kml = lambda *_args, **_kwargs: None

    with tempfile.TemporaryDirectory() as d:
        old_cwd = os.getcwd()
        os.chdir(d)
        try:
            os.makedirs(os.path.join('history', 'notams'), exist_ok=True)
            weekly_csv = os.path.join('history', 'notams', '2026-W13.csv')
            headers = fetch_notams.CSV_HEADERS

            existing_rows = [
                {'country': 'X', 'id': '1', 'notam_id': 'EXPIRED', 'fir': 'F', 'from_utc': '2026-03-20T00:00:00Z', 'to_utc': '2026-03-22T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'expired'},
                {'country': 'X', 'id': '2', 'notam_id': 'VALID_OLD', 'fir': 'F', 'from_utc': '2026-03-24T00:00:00Z', 'to_utc': '2026-03-28T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'valid'},
            ]
            _write_csv(weekly_csv, headers, existing_rows)

            source_csv = os.path.join(d, 'notams.csv')
            new_rows = [
                {'country': 'X', 'id': '3', 'notam_id': 'NEW_VALID', 'fir': 'F', 'from_utc': '2026-03-24T00:00:00Z', 'to_utc': '2026-03-26T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'new valid'},
                {'country': 'X', 'id': '4', 'notam_id': 'TOO_FAR', 'fir': 'F', 'from_utc': '2026-04-10T00:00:00Z', 'to_utc': '2026-04-12T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'too far'},
            ]
            _write_csv(source_csv, headers, new_rows)

            class _FakeDate(datetime.date):
                @classmethod
                def today(cls):
                    return cls(2026, 3, 24)

            original_date = fetch_notams.datetime.date
            fetch_notams.datetime.date = _FakeDate
            try:
                fetch_notams.archive_weekly(source_csv, 'notams')
            finally:
                fetch_notams.datetime.date = original_date

            with open(weekly_csv, 'r', encoding='utf-8') as f:
                merged = list(csv.DictReader(f))
            ids = [r['notam_id'] for r in merged]
            assert 'EXPIRED' not in ids
            assert 'TOO_FAR' not in ids
            assert 'VALID_OLD' in ids
            assert 'NEW_VALID' in ids
        finally:
            os.chdir(old_cwd)


def test_msi_archive_removes_out_of_range():
    now = datetime.datetime(2026, 3, 24, 2, 0, 0)
    fetch_msi.now_utc = now
    fetch_msi.five_days = now + datetime.timedelta(days=5)
    fetch_msi.csv_to_kml = lambda *_args, **_kwargs: None

    with tempfile.TemporaryDirectory() as d:
        old_cwd = os.getcwd()
        os.chdir(d)
        try:
            os.makedirs(os.path.join('history', 'msi'), exist_ok=True)
            weekly_csv = os.path.join('history', 'msi', '2026-W13.csv')
            headers = fetch_msi.CSV_HEADERS

            existing_rows = [
                {'country': 'M', 'id': '', 'notam_id': 'EXPIRED_MSI', 'fir': 'MSI', 'from_utc': '2026-03-20T00:00:00Z', 'to_utc': '2026-03-21T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'expired', 'polygon': '[]'},
                {'country': 'M', 'id': '', 'notam_id': 'VALID_OLD_MSI', 'fir': 'MSI', 'from_utc': '2026-03-24T00:00:00Z', 'to_utc': '2026-03-28T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'valid', 'polygon': '[]'},
            ]
            _write_csv(weekly_csv, headers, existing_rows)

            source_csv = os.path.join(d, 'msi.csv')
            new_rows = [
                {'country': 'M', 'id': '', 'notam_id': 'NEW_VALID_MSI', 'fir': 'MSI', 'from_utc': '2026-03-24T00:00:00Z', 'to_utc': '2026-03-25T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'new valid', 'polygon': '[]'},
                {'country': 'M', 'id': '', 'notam_id': 'TOO_FAR_MSI', 'fir': 'MSI', 'from_utc': '2026-04-10T00:00:00Z', 'to_utc': '2026-04-11T00:00:00Z', 'lat': '1', 'lon': '1', 'radius_nm': '', 'qcode': '', 'raw': 'too far', 'polygon': '[]'},
            ]
            _write_csv(source_csv, headers, new_rows)

            class _FakeDate(datetime.date):
                @classmethod
                def today(cls):
                    return cls(2026, 3, 24)

            original_date = fetch_msi.datetime.date
            fetch_msi.datetime.date = _FakeDate
            try:
                fetch_msi.archive_weekly(source_csv, 'msi')
            finally:
                fetch_msi.datetime.date = original_date

            with open(weekly_csv, 'r', encoding='utf-8') as f:
                merged = list(csv.DictReader(f))
            ids = [r['notam_id'] for r in merged]
            assert 'EXPIRED_MSI' not in ids
            assert 'TOO_FAR_MSI' not in ids
            assert 'VALID_OLD_MSI' in ids
            assert 'NEW_VALID_MSI' in ids
        finally:
            os.chdir(old_cwd)


if __name__ == '__main__':
    test_notams_archive_removes_out_of_range()
    test_msi_archive_removes_out_of_range()
    print('test_history_time_range.py passed')

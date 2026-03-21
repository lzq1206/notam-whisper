import os
import sys
import csv
import tempfile
sys.path.append(os.getcwd())
from fetch_msi import process_msi_data, write_msi_raw_csv


sample = [
    {
        'msgID': 'NOCOORD-1',
        'msgText': 'MSI WITHOUT COORDS 211200Z TO 221200Z MAR 26',
        'category': '14',
        'msgType': 'NW'
    },
    {
        'msgID': 'OLDDATE-1',
        'msgText': 'MSI OLD DATE 12-30N 123-45E 010000Z TO 020000Z JAN 20',
        'category': '14',
        'msgType': 'NW'
    }
]

rows = process_msi_data(sample)

assert len(rows) == 2, f"Expected both rows to be kept without MSI filters, got {len(rows)}"
assert rows[0]['notam_id'] == 'NOCOORD-1'
assert rows[1]['notam_id'] == 'OLDDATE-1'

with tempfile.TemporaryDirectory() as d:
    out_csv = os.path.join(d, 'msi_raw.csv')
    write_msi_raw_csv(rows, out_csv)
    with open(out_csv, 'r', encoding='utf-8') as f:
        written = list(csv.DictReader(f))
    assert len(written) == 2, f"Expected 2 rows in raw CSV, got {len(written)}"
    assert written[0]['notam_id'] == 'NOCOORD-1'
    assert written[1]['notam_id'] == 'OLDDATE-1'
    assert 'MSI WITHOUT COORDS' in written[0]['raw']

print("test_msi_processing passed")

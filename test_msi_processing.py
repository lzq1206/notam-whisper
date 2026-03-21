import os
import sys
sys.path.append(os.getcwd())
from fetch_msi import process_msi_data


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
print("test_msi_processing passed")

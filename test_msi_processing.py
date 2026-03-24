import os
import sys
sys.path.append(os.getcwd())
from fetch_msi import process_msi_data


sample = [
    {
        'msgID': 'NOCOORD-1',
        'msgText': 'ROCKET AREA 12-30.00N 123-45.00E 211200Z TO 221200Z MAR 26',
        'category': '14',
        'msgType': 'NW'
    },
    {
        'msgID': 'OLDDATE-1',
        'msgText': 'LAUNCH WINDOW 13-30.00N 124-45.00E 010000Z TO 020000Z JAN 20',
        'category': '14',
        'msgType': 'NW'
    }
]

rows = process_msi_data(sample)

assert len(rows) == 2, f"Expected both rows to be kept without archive time filters, got {len(rows)}"
assert rows[0]['notam_id'] == 'NOCOORD-1'
assert rows[1]['notam_id'] == 'OLDDATE-1'
assert rows[0]['to_utc'] and rows[1]['to_utc']

print("test_msi_processing passed")

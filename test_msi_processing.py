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

# Category-14 records bypass keyword filter even without aerospace keywords
cat14_sample = [
    {
        'msgID': 'CAT14-1',
        'msgText': 'DANGER AREA EXERCISE 12-30.00N 123-45.00E 211200Z TO 221200Z MAR 26',
        'category': '14',
        'msgType': 'NW'
    }
]
cat14_rows = process_msi_data(cat14_sample)
assert len(cat14_rows) == 1, f"Expected category-14 record to pass without aerospace keyword, got {len(cat14_rows)}"

# Non-category-14 Daily Memo records require a keyword
memo_no_keyword = [
    {
        'msgID': 'MEMO-1',
        'msgText': 'DANGER AREA EXERCISE 14-30.00N 125-45.00E 211200Z TO 221200Z MAR 26',
        'category': 'Daily Memo',
        'msgType': 'NavWarning'
    }
]
memo_no_kw_rows = process_msi_data(memo_no_keyword)
assert len(memo_no_kw_rows) == 0, f"Expected Daily Memo without aerospace keyword to be filtered out, got {len(memo_no_kw_rows)}"

# Non-category-14 record with new keyword SPACECRAFT is kept
spacecraft_sample = [
    {
        'msgID': 'SPACE-1',
        'msgText': 'SPACECRAFT RECOVERY AREA 15-30.00N 126-45.00E 211200Z TO 221200Z MAR 26',
        'category': 'Daily Memo',
        'msgType': 'NavWarning'
    }
]
spacecraft_rows = process_msi_data(spacecraft_sample)
assert len(spacecraft_rows) == 1, f"Expected SPACECRAFT keyword record to be kept, got {len(spacecraft_rows)}"

# Non-category-14 record with new keyword REENTRY (no hyphen) is kept
reentry_sample = [
    {
        'msgID': 'REENTRY-1',
        'msgText': 'REENTRY VEHICLE AREA 16-30.00N 127-45.00E 211200Z TO 221200Z MAR 26',
        'category': 'Daily Memo',
        'msgType': 'NavWarning'
    }
]
reentry_rows = process_msi_data(reentry_sample)
assert len(reentry_rows) == 1, f"Expected REENTRY keyword record to be kept, got {len(reentry_rows)}"

print("test_msi_processing passed")

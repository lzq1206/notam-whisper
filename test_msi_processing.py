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

merged_sample = [
    {
        'msgID': '',
        'msgText': (
            '031744Z FEB 26  HYDROLANT 237/26(54).  AEGEAN SEA.  DNC 09.  1. MISSILE OPERATIONS 0530Z TO SUNSET DAILY EVERY '
            'WEDNESDAY THRU SATURDAY UNTIL 14 JUN AND 16 SEB THRU 31 DEC IN AREAS: '
            'A. 35-36.00N 024-07.00E, 36-18.00N 024-07.00E, 36-18.00N 025-59.00E, 36-25.00N 026-12.00E. '
            '2. CANCEL THIS MSG 010001Z JAN 27. '
            '040658Z MAR 26  NAVAREA IV 207/26(11,26).  NORTH ATLANTIC.  FLORIDA.  1. HAZARDOUS OPERATIONS, ROCKET LAUNCHING '
            '100304Z TO 100636Z MAR, ALTERNATE 0304Z TO 0636Z DAILY 11 THRU 16 MAR IN AREAS BOUND BY: '
            'A. 29-01.00N 075-00.00W, 29-21.00N 073-51.00W, 29-20.00N 072-05.00W, 28-29.00N 072-05.00W. '
            '2. CANCEL THIS MSG 160736Z MAR 26.'
        ),
        'category': '14',
        'msgType': 'NavWarning'
    }
]
merged_rows = process_msi_data(merged_sample)
assert len(merged_rows) == 2, f"Expected concatenated warnings to split into 2 rows, got {len(merged_rows)}"
assert merged_rows[0]['notam_id'].startswith('HYDROLANT 237/26'), merged_rows[0]['notam_id']
assert merged_rows[1]['notam_id'].startswith('NAVAREA IV 207/26'), merged_rows[1]['notam_id']

print("test_msi_processing passed")

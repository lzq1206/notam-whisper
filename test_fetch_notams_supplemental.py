#!/usr/bin/env python3
from fetch_notams import (
    FAA_SUPPLEMENTAL_FIRS,
    _normalize_notam_number,
    _parse_q_line,
    merge_notams,
)


def test_normalize_notam_number():
    assert _normalize_notam_number('A0787/26') == ('A', 787, '2026')
    assert _normalize_notam_number('Q3520/2026') == ('Q', 3520, '2026')
    assert _normalize_notam_number('invalid') == ('', '', '')


def test_parse_q_line():
    raw = (
        "A0787/26 NOTAMN Q) ZXXX/QRDCA/IV/BO/W/000/999/3349N11036E013 "
        "A) ZLHW ZHWH B) 2603252243 C) 2603252305 E) ... F) SFC G) UNL"
    )
    lat, lon, radius, qcode = _parse_q_line(raw)
    assert qcode == 'QRDCA'
    assert abs(lat - 33.816667) < 1e-5
    assert abs(lon - 110.6) < 1e-5
    assert radius == '013'


def test_merge_notams_dedupes_by_notam_id():
    primary = [
        {'id': '1', 'notam': {'series': 'A', 'number': 787, 'year': '2026', 'raw': 'x'}},
        {'id': '2', 'notam': {'series': 'A', 'number': 760, 'year': '2026', 'raw': 'y'}},
    ]
    supplemental = [
        {'id': '9', 'notam': {'series': 'A', 'number': 787, 'year': '2026', 'raw': 'new'}},
        {'id': '3', 'notam': {'series': 'A', 'number': 761, 'year': '2026', 'raw': 'z'}},
    ]
    merged = merge_notams(primary, supplemental)
    assert [item['id'] for item in merged] == ['1', '2', '3']

def test_faa_supplemental_firs_include_zxxx():
    assert 'ZXXX' in FAA_SUPPLEMENTAL_FIRS


if __name__ == '__main__':
    test_normalize_notam_number()
    test_parse_q_line()
    test_merge_notams_dedupes_by_notam_id()
    test_faa_supplemental_firs_include_zxxx()
    print('test_fetch_notams_supplemental.py passed')

#!/usr/bin/env python3
from fetch_notams import (
    COUNTRY_FETCH_RETRIES,
    FAA_SUPPLEMENTAL_FIRS,
    fetch_global_notam_supplement,
    _normalize_notam_number,
    _parse_q_line,
    _passes_filters,
    fetch_country,
    fetch_notammap,
    merge_notams,
    _build_straight_line_corridor_polygon,
    _correlate_silent_launch_notam,
)
import datetime


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
    assert 'ZBPE' in FAA_SUPPLEMENTAL_FIRS
    assert 'RPHI' in FAA_SUPPLEMENTAL_FIRS
    assert 'UACN' in FAA_SUPPLEMENTAL_FIRS


def test_fetch_country_retries_retryable_status():
    import fetch_notams

    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    calls = {'count': 0}

    def fake_get(url, headers=None, timeout=None):
        calls['count'] += 1
        if calls['count'] < COUNTRY_FETCH_RETRIES:
            return FakeResponse(503)
        return FakeResponse(200, {'notams': [{'id': 'ok'}]})

    original_get = fetch_notams.requests.get
    original_sleep = fetch_notams.time.sleep
    fetch_notams.requests.get = fake_get
    fetch_notams.time.sleep = lambda _: None
    try:
        result = fetch_country('Testland')
    finally:
        fetch_notams.requests.get = original_get
        fetch_notams.time.sleep = original_sleep

    assert calls['count'] == COUNTRY_FETCH_RETRIES
    assert result == [{'id': 'ok'}]


def test_fetch_country_returns_empty_list_by_default_after_retries():
    import fetch_notams

    class FakeResponse:
        def __init__(self, status_code):
            self.status_code = status_code

        def json(self):
            return {}

    def fake_get(url, headers=None, timeout=None):
        return FakeResponse(503)

    original_get = fetch_notams.requests.get
    original_sleep = fetch_notams.time.sleep
    fetch_notams.requests.get = fake_get
    fetch_notams.time.sleep = lambda _: None
    try:
        result = fetch_country('AlwaysFailLand')
    finally:
        fetch_notams.requests.get = original_get
        fetch_notams.time.sleep = original_sleep

    assert result == []


def test_fetch_notammap_sequential_retry_for_failed_country():
    import fetch_notams

    attempts = {'A': 0, 'B': 0}

    def fake_fetch_countries():
        return ['A', 'B']

    def fake_fetch_country(country, report_failure=False):
        attempts[country] += 1
        if country == 'A' and attempts[country] == 1:
            return None
        return [{'id': country, 'notam': {'raw': 'ROCKET UNL', 'from': '', 'to': ''}}]

    original_fetch_countries = fetch_notams.fetch_countries
    original_fetch_country = fetch_notams.fetch_country
    fetch_notams.fetch_countries = fake_fetch_countries
    fetch_notams.fetch_country = fake_fetch_country
    try:
        items = fetch_notammap()
    finally:
        fetch_notams.fetch_countries = original_fetch_countries
        fetch_notams.fetch_country = original_fetch_country

    assert attempts['A'] == 2
    assert attempts['B'] == 1
    assert sorted(item['id'] for item in items) == ['A', 'B']


def test_fetch_country_fallback_slug_handles_accents_and_apostrophe():
    import fetch_notams

    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    urls = []

    def fake_get(url, headers=None, timeout=None):
        urls.append(url)
        if url.endswith('Cote_dIvoire.json'):
            return FakeResponse(200, {'notams': [{'id': 'ok'}]})
        return FakeResponse(404)

    original_get = fetch_notams.requests.get
    original_sleep = fetch_notams.time.sleep
    fetch_notams.requests.get = fake_get
    fetch_notams.time.sleep = lambda _: None
    try:
        result = fetch_country("Côte d'Ivoire")
    finally:
        fetch_notams.requests.get = original_get
        fetch_notams.time.sleep = original_sleep

    assert result == [{'id': 'ok'}]
    assert len(urls) >= 2
    assert any(url.endswith('Cote_dIvoire.json') for url in urls)


def test_passes_filters_rejects_generic_unl_and_qcodes_without_launch_evidence():
    assert _passes_filters({
        'raw': 'A TEMPORARY DANGER AREA ESTABLISHED BOUNDED BY ...',
        'notamCode': 'QRDCA',
        'from': '',
        'to': '',
    }) is False

    assert _passes_filters({
        'raw': (
            'K0854/26 TEMPORARY RESTRICTED AREA (ARCA 14) ACT. '
            'LDG/TKOF OPS PPR. F) GND G) UNL'
        ),
        'notamCode': 'QRTCA',
        'from': '',
        'to': '',
    }) is False

    assert _passes_filters({
        'raw': 'FRNG (LIVE FIRE) WILL TAKE PLACE F) GND G) FL330',
        'notamCode': 'QWMLW',
        'from': '',
        'to': '',
    }) is False

    assert _passes_filters({
        'raw': 'A TEMPORARY DANGER AREA ESTABLISHED BOUNDED BY ... F) GND G) UNL',
        'notamCode': 'QRDCA',
        '_trusted_launch_source': True,
        'from': '',
        'to': '',
    }) is True

    assert _passes_filters({
        'raw': 'SPECIAL OPS (AEROSPACE FLT ACT) WITH EST FALL AREA OF UNBURNED DEBRIS',
        'notamCode': 'QWMLW',
        'from': '',
        'to': '',
    }) is True


def test_passes_filters_does_not_treat_airspace_as_space_keyword():
    assert _passes_filters({
        'raw': 'TEMPORARY RESTRICTED AIRSPACE FOR ROUTINE TRAINING',
        'notamCode': 'QRTCA',
        'from': '',
        'to': '',
    }) is False


def test_silent_baikonur_launch_notams_require_time_and_space_correlation():
    launch = [{
        'mission': 'Soyuz MS-29',
        'time': datetime.datetime(2026, 7, 14, 14, 47),
        'site': 'Baikonur Cosmodrome',
        'lat': 45.964,
        'lon': 63.305,
    }]
    raw = (
        'K1569/26 NOTAMN Q) UAXX/QRPCA/IV/NBO/W /000/999/4859N07306E435 '
        'A) UACN UAAA UAII B) 2607141447 C) 2607141522 '
        'E) PROHIBITED AREA ACTIVATED 40.5 NM EITHER SIDE OF A STRAIGHT LINE '
        'DEFINED BY: 455946N 0633351E - 473200N 0680000E - '
        '503000N 0801200E - 505600N 0834100E F) GND G) UNL'
    )
    notam = {
        'raw': raw,
        'notamCode': 'QRPCA',
        'from': '2026-07-14T14:47:00Z',
        'to': '2026-07-14T15:22:00Z',
        'latitude': 48.983333,
        'longitude': 73.1,
        'radius': '',
    }

    assert _passes_filters(dict(notam)) is False
    assert _correlate_silent_launch_notam(notam, launch) == 'Soyuz MS-29'
    assert _passes_filters(notam) is True
    assert len(notam['polygon']) == 8

    unrelated_time = dict(notam)
    unrelated_time.pop('_trusted_launch_source', None)
    unrelated_time.pop('_launch_match', None)
    unrelated_time['from'] = '2026-07-14T16:13:00Z'
    unrelated_time['to'] = '2026-07-14T16:43:00Z'
    assert _correlate_silent_launch_notam(unrelated_time, launch) == ''
    assert _passes_filters(unrelated_time) is False

    daily_area = dict(notam)
    daily_area.pop('_trusted_launch_source', None)
    daily_area.pop('_launch_match', None)
    daily_area['raw'] = (
        'C4542/26 NOTAMN Q) UATT/QRPCA/IV/NBO/W /000/999/4824N04814E061 '
        'A) UATT B) 2607140600 C) 2607191400 D) DAILY 0600-1400 '
        'E) PROHIBITED AREA ACTIVATED F) GND G) UNL'
    )
    daily_area['from'] = '2026-07-14T06:00:00Z'
    daily_area['to'] = '2026-07-19T14:00:00Z'
    daily_area['latitude'] = 48.4
    daily_area['longitude'] = 48.233333
    assert _correlate_silent_launch_notam(daily_area, launch) == ''


def test_named_danger_area_can_match_launch_without_rocket_keyword():
    launch = [{
        'mission': 'Soyuz MS-29',
        'time': datetime.datetime(2026, 7, 14, 14, 47),
        'site': 'Baikonur Cosmodrome',
        'lat': 45.964,
        'lon': 63.305,
    }]
    notam = {
        'raw': (
            'K1599/26 NOTAMN Q) UACN/QRDCA/IV/BO /W /000/999/4723N06722E022 '
            'A) UACN B) 2607141430 C) 2607181630 '
            'E) DANGER AREA UAD26 ACTIVATED. F) GND G) UNL'
        ),
        'notamCode': 'QRDCA',
        'from': '2026-07-14T14:30:00Z',
        'to': '2026-07-18T16:30:00Z',
        'latitude': 47.383333,
        'longitude': 67.366667,
        'radius': '022',
    }
    assert _correlate_silent_launch_notam(notam, launch) == 'Soyuz MS-29'
    assert _passes_filters(notam) is True


def test_straight_line_corridor_geometry_is_buffered_not_centerline():
    raw = (
        'E) PROHIBITED AREA ACTIVATED 40.5 NM EITHER SIDE OF A STRAIGHT LINE '
        'DEFINED BY: 455946N 0633351E - 473200N 0680000E - '
        '503000N 0801200E - 505600N 0834100E F) GND G) UNL'
    )
    polygon = _build_straight_line_corridor_polygon(raw)
    assert len(polygon) == 8
    assert polygon[0] != [45.996111, 63.564167]


def test_raw_coordinate_parser_keeps_f2572_as_one_three_vertex_ring():
    from fetch_notams import _extract_raw_coordinate_rings

    raw = (
        'F2572/26 NOTAMN Q) YMMM/QWMLW/IV/BO/W/000/999/6230S16247E020 '
        'A) YMMM E) CHARACTERISTICS OF IMPACT AREA: '
        '621200S 1630000E 624100S 1630000E 623300S 1623336E '
        '621200S 1630000E F) SFC G) UNL'
    )
    rings = _extract_raw_coordinate_rings(raw)
    assert rings == [[[-62.2, 163.0], [-62.683333, 163.0], [-62.55, 162.56]]]


def test_raw_coordinate_parser_splits_a5852_into_two_closed_rings():
    from fetch_notams import _extract_raw_coordinate_rings

    raw = (
        'A5852/26 NOTAMN Q) ENOB/QRDCA/IV/BO/W/000/999/7540N02149E051 '
        'A) ENOB E) ACTIVATED PSN 763000N 0220000E - 752000N 0244000E - '
        '745000N 0214000E - 755000N 0184000E - 763000N 0220000E - '
        '(763000N 0220000E) IMPACT AREA. SIMILAR ACTIVITIES AT PSN '
        '705600N 0320500E - 701000N 0320500E - 700928N 0320152E - '
        '701500N 0315000E - 703007N 0315000E - 703622N 0314318E - '
        '705600N 0320500E - (705600N 0320500E) F) GND G) UNL'
    )
    rings = _extract_raw_coordinate_rings(raw)
    assert [len(ring) for ring in rings] == [4, 6]
    assert rings[0][0] == [76.5, 22.0]
    assert rings[1][0] == [70.933333, 32.083333]


def test_fetch_notammap_exits_when_country_list_empty():
    """fetch_notammap() must abort (sys.exit(1)) when notammap.org returns no countries."""
    import fetch_notams

    def fake_fetch_countries():
        return []

    original_fetch_countries = fetch_notams.fetch_countries
    fetch_notams.fetch_countries = fake_fetch_countries
    try:
        exited = False
        try:
            fetch_notams.fetch_notammap()
        except SystemExit as e:
            exited = True
            assert e.code == 1, f"Expected exit code 1, got {e.code}"
        assert exited, "fetch_notammap() should have called sys.exit(1) for empty country list"
    finally:
        fetch_notams.fetch_countries = original_fetch_countries


def test_fetch_faa_notams_logs_non_200():
    """fetch_faa_notams() logs a message when the FAA endpoint returns non-200."""
    import fetch_notams
    import io
    from contextlib import redirect_stdout

    class FakeResponse:
        def __init__(self, status_code):
            self.status_code = status_code

    original_session_cls = fetch_notams.requests.Session

    class _Session:
        headers = type('H', (), {'update': lambda self, h: None})()
        def post(self, url, data=None, timeout=None):
            return FakeResponse(503)

    fetch_notams.requests.Session = lambda: _Session()
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            fetch_notams.fetch_faa_notams()
    finally:
        fetch_notams.requests.Session = original_session_cls

    output = buf.getvalue()
    assert 'Non-200' in output or '503' in output, (
        f"Expected non-200 log in output, got: {output!r}"
    )


def test_fetch_faa_notams_logs_non_json_response():
    """fetch_faa_notams() logs a message when the FAA endpoint returns non-JSON."""
    import fetch_notams
    import io
    from contextlib import redirect_stdout

    class FakeResponse:
        status_code = 200
        def json(self):
            raise ValueError("not valid JSON")

    class _Session:
        headers = type('H', (), {'update': lambda self, h: None})()
        def post(self, url, data=None, timeout=None):
            return FakeResponse()

    original_session_cls = fetch_notams.requests.Session
    fetch_notams.requests.Session = lambda: _Session()
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            fetch_notams.fetch_faa_notams()
    finally:
        fetch_notams.requests.Session = original_session_cls

    output = buf.getvalue()
    assert 'Non-JSON' in output or 'not valid JSON' in output, (
        f"Expected non-JSON log in output, got: {output!r}"
    )


def test_global_supplement_parses_key_notams():
    import fetch_notams

    class FakeResponse:
        status_code = 200
        def json(self):
            return {
                'features': [
                    {
                        'id': 'x1',
                        'eventType': 'launch',
                        'country': 'china',
                        'name': 'A1417/26',
                        'description': (
                            '05/03/2026 0502 A1417/26 NOTAMN\n'
                            'Q)ZBPE/QRDCA/IV/BO/W/000/999/3554N11209E013\n'
                            'A)ZBPE B)2608121149 C)2608121209\n'
                            'E) A TEMPORARY DANGER AREA ESTABLISHED BOUNDED BY: ...'
                        )
                    },
                    {
                        'id': 'x2',
                        'eventType': 'missile',
                        'country': 'philippines',
                        'name': 'B1768/26',
                        'description': (
                            '04/28/2026 0358 B1768/26 NOTAMN\n'
                            'Q) RPHI/QWMLW/IV/BO /W /000/999/1921N11925E047\n'
                            'A) RPHI B) 2608011600 C) 2608052200\n'
                            'E) SPECIAL OPS (AEROSPACE FLT ACT) WILL BE CONDUCTED BY CHINA.'
                        )
                    },
                    {
                        'id': 'x3',
                        'eventType': 'other',
                        'country': 'china',
                        'name': 'IGNORED',
                        'description': 'IGNORED'
                    }
                ]
            }

    original_get = fetch_notams.requests.get
    fetch_notams.requests.get = lambda *args, **kwargs: FakeResponse()
    try:
        rows = fetch_global_notam_supplement()
    finally:
        fetch_notams.requests.get = original_get

    ids = {f"{r['notam']['series']}{r['notam']['number']}/{r['notam']['year'][-2:]}" for r in rows}
    assert 'A1417/26' in ids
    assert 'B1768/26' in ids
    assert all(r['notam']['notamCode'] in {'QRDCA', 'QWMLW'} for r in rows)


def test_global_supplement_parses_current_joey_data_dict_schema():
    import fetch_notams

    raws = [
        (
            'B3426/26 NOTAMN\nQ) RPHI/QWMLW/IV/BO /W /000/999/1245N11610E038\n'
            'A) RPHI B) 2607241100 C) 2607301400\n'
            'E) SPECIAL OPS (AEROSPACE FLT ACT) WILL BE CONDUCTED BY CHINA.\nF) SFC G) UNL'
        ),
        (
            'B3427/26 NOTAMN\nQ) RPHI/QWMLW/IV/BO /W /000/999/0811N11926E046\n'
            'A) RPHI B) 2607241100 C) 2607301400\n'
            'E) SPECIAL OPS (AEROSPACE FLT ACT) WILL BE CONDUCTED BY CHINA.\nF) SFC G) UNL'
        ),
        (
            'P3221/26 NOTAMN\nQ)RJJJ/QXXXX/IV/NBO/E/000/999/2208N12739E029\n'
            'A)RJJJ B)2607231100 C)2607261400\n'
            'E)DUE TO AN AEROSPACE FLIGHT ACTIVITY.\nF)SFC G)UNL'
        ),
    ]
    payload = {
        'NOTAM_DATA': {
            'CODE': ['B3426/26', 'B3427/26', 'P3221/26'],
            'COORDINATES': [
                'N130200E1153700-N132100E1160400-N122800E1164300-N121000E1161600-N130200E1153700',
                'N083500E1184800-N085400E1191500-N084300E1192900-N075000E1200600-N072600E1193000-N081900E1185300-N083500E1184800',
                'N2206E12809-N2225E12715-N2210E12709-N2151E12803-N2206E12809',
            ],
            'PLATID': ['81760689', '81760830', '81648331'],
            'RAWMESSAGE': raws,
            'SOURCE': ['NOTAM', 'NOTAM', 'NOTAM'],
            'FIR': ['RPHI', 'RPHI', 'RJJJ'],
        }
    }

    class FakeResponse:
        status_code = 200
        def json(self):
            return payload

    original_get = fetch_notams.requests.get
    original_window = fetch_notams._is_in_time_window
    fetch_notams.requests.get = lambda *args, **kwargs: FakeResponse()
    fetch_notams._is_in_time_window = lambda *_: True
    try:
        rows = fetch_notams.fetch_global_notam_supplement()
    finally:
        fetch_notams.requests.get = original_get
        fetch_notams._is_in_time_window = original_window

    by_id = {row['notam']['notam_id']: row for row in rows}
    assert set(by_id) == {'B3426/26', 'B3427/26', 'P3221/26'}
    assert len(by_id['B3426/26']['notam']['polygon']) == 4
    assert len(by_id['B3427/26']['notam']['polygon']) == 6
    assert len(by_id['P3221/26']['notam']['polygon']) == 4
    assert by_id['B3426/26']['notam']['polygon'][0] == [13.033333, 115.616667]
    assert by_id['P3221/26']['notam']['polygon'][0] == [22.1, 128.15]
    assert by_id['B3426/26']['_country'] == 'PHILIPPINES'
    assert by_id['P3221/26']['_country'] == 'JAPAN'


def test_parse_faa_space_tfr_detail():
    import fetch_notams

    summary = {
        'notam_id': '6/5192',
        'facility': 'ZJX',
        'type': 'SPACE OPERATIONS',
        'description': 'Cape Canaveral, FL, Wednesday, July 15, 2026 Local',
    }
    detail = [{
        'notam_id': '6/5192',
        'text': (
            '<table><tr><td>Beginning Date and Time :</td><td>July 15, 2026 at 0643 UTC</td></tr>'
            '<tr><td>Ending Date and Time :</td><td>July 15, 2026 at 1135 UTC</td></tr>'
            '<tr><td>Reason for NOTAM :</td><td>Space Operations Area</td></tr></table>'
            'Region bounded by: From: 28º51\'16"N 80º42\'19"W '
            'To: 29º07\'30"N 80º30\'00"W '
            'Clockwise on a 30 NM ARC Centered on: 28º37\'03"N 80º36\'47"W '
            'To: 28º13\'30"N 80º16\'00"W '
            'To: 28º25\'01"N 80º30\'29"W '
            'To: 28º51\'16"N 80º42\'19"W Altitude: Surface to 18000 feet. '
            'Authority: Title 14 CFR section 91.143'
        ),
    }]
    original_window = fetch_notams._is_in_time_window
    fetch_notams._is_in_time_window = lambda *_: True
    try:
        row = fetch_notams._parse_faa_tfr_detail(summary, detail)
    finally:
        fetch_notams._is_in_time_window = original_window

    assert row['notam']['notam_id'] == 'FDC 6/5192'
    assert row['notam']['fir'] == 'ZJX'
    assert row['notam']['from'] == '2026-07-15T06:43:00Z'
    assert row['notam']['to'] == '2026-07-15T11:35:00Z'
    assert '285116N 0804219W' in row['notam']['raw']
    assert row['notam']['notamCode'] == 'TFR91.143'
    assert row['_country'] == 'USA'
    polygon = row['notam']['polygon']
    assert len(polygon) > 10
    assert polygon[0] == [28.854444, -80.705278]
    assert polygon[1] == [29.125, -80.5]
    assert [28.225, -80.266667] in polygon
    assert [28.6175, -80.613056] not in polygon, 'FAA arc center must not be used as a boundary vertex'


if __name__ == '__main__':
    test_normalize_notam_number()
    test_parse_q_line()
    test_merge_notams_dedupes_by_notam_id()
    test_faa_supplemental_firs_include_zxxx()
    test_fetch_country_retries_retryable_status()
    test_fetch_country_returns_empty_list_by_default_after_retries()
    test_fetch_notammap_sequential_retry_for_failed_country()
    test_fetch_country_fallback_slug_handles_accents_and_apostrophe()
    test_passes_filters_rejects_generic_unl_and_qcodes_without_launch_evidence()
    test_passes_filters_does_not_treat_airspace_as_space_keyword()
    test_fetch_notammap_exits_when_country_list_empty()
    test_fetch_faa_notams_logs_non_200()
    test_fetch_faa_notams_logs_non_json_response()
    test_global_supplement_parses_key_notams()
    test_global_supplement_parses_current_joey_data_dict_schema()
    test_parse_faa_space_tfr_detail()
    print('test_fetch_notams_supplemental.py passed')

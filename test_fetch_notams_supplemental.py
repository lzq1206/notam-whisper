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
    assert 'ZBPE' in FAA_SUPPLEMENTAL_FIRS
    assert 'RPHI' in FAA_SUPPLEMENTAL_FIRS


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


def test_passes_filters_accepts_qrdca_without_keep_keyword():
    assert _passes_filters({
        'raw': 'A TEMPORARY DANGER AREA ESTABLISHED BOUNDED BY ...',
        'notamCode': 'QRDCA',
        'from': '',
        'to': '',
    }) is True

    assert _passes_filters({
        'raw': 'A TEMPORARY DANGER AREA ESTABLISHED BOUNDED BY ...',
        'notamCode': 'QXXXX',
        'from': '',
        'to': '',
    }) is False


def test_passes_filters_does_not_treat_airspace_as_space_keyword():
    assert _passes_filters({
        'raw': 'TEMPORARY RESTRICTED AIRSPACE FOR ROUTINE TRAINING',
        'notamCode': 'QRTCA',
        'from': '',
        'to': '',
    }) is False


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
            'From: 28º51\'16"N 80º42\'19"W To: 29º07\'30"N 80º30\'00"W '
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


if __name__ == '__main__':
    test_normalize_notam_number()
    test_parse_q_line()
    test_merge_notams_dedupes_by_notam_id()
    test_faa_supplemental_firs_include_zxxx()
    test_fetch_country_retries_retryable_status()
    test_fetch_country_returns_empty_list_by_default_after_retries()
    test_fetch_notammap_sequential_retry_for_failed_country()
    test_fetch_country_fallback_slug_handles_accents_and_apostrophe()
    test_passes_filters_accepts_qrdca_without_keep_keyword()
    test_passes_filters_does_not_treat_airspace_as_space_keyword()
    test_fetch_notammap_exits_when_country_list_empty()
    test_fetch_faa_notams_logs_non_200()
    test_fetch_faa_notams_logs_non_json_response()
    test_global_supplement_parses_key_notams()
    test_parse_faa_space_tfr_detail()
    print('test_fetch_notams_supplemental.py passed')

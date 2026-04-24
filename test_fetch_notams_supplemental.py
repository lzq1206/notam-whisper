#!/usr/bin/env python3
from fetch_notams import (
    COUNTRY_FETCH_RETRIES,
    FAA_SUPPLEMENTAL_FIRS,
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
    print('test_fetch_notams_supplemental.py passed')

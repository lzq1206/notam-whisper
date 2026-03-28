import os
import sys
sys.path.append(os.getcwd())
import fetch_msi


class StubResponse:
    def __init__(self, text, status=200):
        self.text = text
        self.status_code = status


def run_with_stub(stub_get, func, *args):
    old_get = fetch_msi.requests.get
    old_sleep = fetch_msi.time.sleep
    try:
        fetch_msi.requests.get = stub_get
        fetch_msi.time.sleep = lambda *_: None
        return func(*args)
    finally:
        fetch_msi.requests.get = old_get
        fetch_msi.time.sleep = old_sleep


def test_fetch_txt_url_parses_blocks():
    txt = (
        "260853Z FEB 26\n"
        "NAVAREA IV ROCKET LAUNCH AREA 12-30.00N 123-45.00E\n"
        "211200Z TO 221200Z MAR 26\n"
        "\n"
        "SHORT BLOCK\n"
        "\n"
        "260900Z FEB 26\n"
        "NAVAREA IV LAUNCH AREA 13-00.00N 124-00.00E\n"
        "230000Z TO 240000Z MAR 26"
    )

    def stub_get(*args, **kwargs):
        return StubResponse(txt, 200)

    url = fetch_msi.TXT_URLS[0]
    rows = run_with_stub(stub_get, fetch_msi.fetch_txt_url, url)
    assert len(rows) == 2, f"Expected 2 blocks (the 'SHORT BLOCK' is under the {fetch_msi.MIN_BLOCK_LENGTH}-char threshold), got {len(rows)}"
    assert rows[0]['category'] == 'Daily Memo'
    assert rows[0]['msgType'] == 'NavWarning'
    assert 'ROCKET' in rows[0]['msgText']


def test_fetch_txt_url_handles_http_error():
    def stub_get(*args, **kwargs):
        return StubResponse("", 503)

    rows = run_with_stub(stub_get, fetch_msi.fetch_txt_url, fetch_msi.TXT_URLS[0])
    assert rows == [], f"Expected empty list on HTTP error, got {rows}"


def test_fetch_html_url_extracts_text():
    html = (
        "<html><head><title>MSI</title></head><body>"
        "<table>"
        "<tr><td>260853Z FEB 26 NAVAREA IV ROCKET LAUNCH AREA 12-30.00N 123-45.00E "
        "211200Z TO 221200Z MAR 26</td></tr>"
        "</table>"
        "<script>var x = 1;</script>"
        "</body></html>"
    )

    def stub_get(*args, **kwargs):
        return StubResponse(html, 200)

    url = fetch_msi.HTML_URLS[0]
    rows = run_with_stub(stub_get, fetch_msi.fetch_html_url, url)
    assert len(rows) >= 1, f"Expected at least 1 block from HTML, got {rows}"
    assert rows[0]['category'] == '14'
    assert rows[0]['msgType'] == 'NavWarning'
    combined = ' '.join(r['msgText'] for r in rows)
    assert 'ROCKET' in combined or 'LAUNCH' in combined or 'NAVAREA' in combined


def test_fetch_html_url_strips_script_content():
    html = (
        "<html><body>"
        "<script>THIS SHOULD NOT APPEAR IN OUTPUT</script>"
        "<p>260900Z FEB 26\n"
        "NAVAREA IV SATELLITE AREA 13-00.00N 124-00.00E\n"
        "230000Z TO 240000Z MAR 26</p>"
        "</body></html>"
    )

    def stub_get(*args, **kwargs):
        return StubResponse(html, 200)

    rows = run_with_stub(stub_get, fetch_msi.fetch_html_url, fetch_msi.HTML_URLS[0])
    combined = ' '.join(r['msgText'] for r in rows)
    assert 'THIS SHOULD NOT APPEAR IN OUTPUT' not in combined


def test_fetch_html_url_handles_http_error():
    def stub_get(*args, **kwargs):
        return StubResponse("", 503)

    rows = run_with_stub(stub_get, fetch_msi.fetch_html_url, fetch_msi.HTML_URLS[0])
    assert rows == [], f"Expected empty list on HTTP error, got {rows}"


test_fetch_txt_url_parses_blocks()
test_fetch_txt_url_handles_http_error()
test_fetch_html_url_extracts_text()
test_fetch_html_url_strips_script_content()
test_fetch_html_url_handles_http_error()
print("test_msi_fetch_single passed")

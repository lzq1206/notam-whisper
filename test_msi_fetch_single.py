import os
import sys
sys.path.append(os.getcwd())
import fetch_msi


class DummyResp:
    def __init__(self, text, status=200, headers=None):
        self.text = text
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


def run_with_stub(stub_get):
    old_get = fetch_msi.requests.get
    old_sleep = fetch_msi.time.sleep
    try:
        fetch_msi.requests.get = stub_get
        fetch_msi.time.sleep = lambda *_: None
        return fetch_msi.fetch_msi_single('4')
    finally:
        fetch_msi.requests.get = old_get
        fetch_msi.time.sleep = old_sleep


def test_accepts_xml_and_extracts_entities():
    xml = """<root><smapsActiveEntity><msgID>A</msgID><msgText>T</msgText><category>14</category><msgType>NW</msgType></smapsActiveEntity></root>"""

    def stub_get(*args, **kwargs):
        return DummyResp(xml, 200, {'content-type': 'application/xml'})

    rows = run_with_stub(stub_get)
    assert len(rows) == 1
    assert rows[0]['msgID'] == 'A'
    assert rows[0]['msgText'] == 'T'


def test_rejects_html_like_response():
    def stub_get(*args, **kwargs):
        return DummyResp("<html><body>blocked</body></html>", 200, {'content-type': 'text/html'})

    rows = run_with_stub(stub_get)
    assert rows == [], f"Expected no rows for HTML response, got {rows}"


test_accepts_xml_and_extracts_entities()
test_rejects_html_like_response()
print("test_msi_fetch_single passed")

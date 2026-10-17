#!/usr/bin/env python3
"""Render widget/index.html to widget/china-square.png using local repo data."""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "widget" / "china-square.png"
SIZE = 1024


class QuietHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, *_args):
        pass


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/widget/render.html"
    browser_path = os.environ.get("CHROME_BIN")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, executable_path=browser_path or None, args=["--no-sandbox"])
            page = browser.new_page(viewport={"width": SIZE, "height": SIZE}, device_scale_factor=1)
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_function("window.widgetReady === true", timeout=90000)
            page.wait_for_timeout(5000)  # Allow imagery tiles to finish painting.
            page.wait_for_function("document.querySelectorAll('.leaflet-tile-loaded').length >= 4", timeout=60000)
            page.screenshot(path=str(OUTPUT), type="png", animations="disabled")
            page.close()
            browser.close()
    finally:
        server.shutdown()
        server.server_close()

    if not OUTPUT.exists() or OUTPUT.stat().st_size < 20_000:
        raise RuntimeError(f"Screenshot missing or unexpectedly small: {OUTPUT}")
    from PIL import Image
    with Image.open(OUTPUT) as image:
        if image.size != (SIZE, SIZE):
            raise RuntimeError(f"Expected {SIZE}x{SIZE} PNG, got {image.size}")
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "image": "china-square.png",
        "width": SIZE,
        "height": SIZE,
    }
    (ROOT / "widget" / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {OUTPUT.relative_to(ROOT)} ({OUTPUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()

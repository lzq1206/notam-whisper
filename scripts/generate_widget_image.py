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
SMALL_OUTPUT = ROOT / "widget" / "china-widget-small.png"
LARGE_OUTPUT = ROOT / "widget" / "china-widget-large.png"


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
        source = image.convert("RGB")
        resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
        source.resize((338, 338), resampling).save(SMALL_OUTPUT, format="PNG", optimize=True)
        # iPhone large widget is about 360x376 pt; render at @2x. Preserve the
        # complete square map and letterbox the slight height difference.
        large_map = source.resize((720, 720), resampling)
        large_canvas = Image.new("RGB", (720, 752), "#07141e")
        large_canvas.paste(large_map, (0, 16))
        large_canvas.save(LARGE_OUTPUT, format="PNG", optimize=True)
    with Image.open(SMALL_OUTPUT) as image:
        if image.size != (338, 338):
            raise RuntimeError(f"Expected 338x338 small widget PNG, got {image.size}")
    with Image.open(LARGE_OUTPUT) as image:
        if image.size != (720, 752):
            raise RuntimeError(f"Expected 720x752 large widget PNG, got {image.size}")
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "image": "china-square.png",
        "width": SIZE,
        "height": SIZE,
        "widget_images": {
            "small": {"image": SMALL_OUTPUT.name, "width": 338, "height": 338},
            "large": {"image": LARGE_OUTPUT.name, "width": 720, "height": 752},
        },
    }
    (ROOT / "widget" / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {OUTPUT.relative_to(ROOT)} ({OUTPUT.stat().st_size} bytes)")
    print(f"Widget PNGs: {SMALL_OUTPUT.stat().st_size} bytes small, {LARGE_OUTPUT.stat().st_size} bytes large")


if __name__ == "__main__":
    main()

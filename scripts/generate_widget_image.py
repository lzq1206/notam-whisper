#!/usr/bin/env python3
"""Render the China rocket widget's 1024px preview and 3x iPhone images.

Inputs: widget/render.html plus local launch, NOTAM, MSI and KML data in repo root.
Outputs: widget/china-square.png and 3x small/medium/large JPEG variants plus manifest.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
PREVIEW = ROOT / "widget" / "china-square.png"
SMALL = ROOT / "widget" / "china-widget-small.jpg"
MEDIUM = ROOT / "widget" / "china-widget-medium.jpg"
LARGE = ROOT / "widget" / "china-widget-large.jpg"
PREVIEW_SIZE = (1024, 1024)
SMALL_SIZE = (507, 507)       # 169 x 169 pt at 3x
MEDIUM_SIZE = (1080, 507)     # 360 x 169 pt at 3x
LARGE_SIZE = (1092, 1146)     # 364 x 382 pt at 3x, original large-widget proportion


class QuietHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, *_args):
        pass


def save_jpeg(image: Image.Image, path: Path) -> None:
    # Keep each widget JPEG small enough to persist as ScriptWidget's base64
    # storage value (256 KiB limit), with margin for image-to-image variation.
    image.convert("RGB").save(path, format="JPEG", quality=68, optimize=True, progressive=True, subsampling=2)


def render_page(browser, url: str, viewport: tuple[int, int], output: Path) -> Image.Image:
    page = browser.new_page(viewport={"width": viewport[0], "height": viewport[1]}, device_scale_factor=1)
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_function("window.widgetReady === true", timeout=90000)
    page.wait_for_timeout(2500)
    page.wait_for_function("document.querySelectorAll('.leaflet-tile-loaded').length >= 4", timeout=60000)
    page.screenshot(path=str(output), type="png", animations="disabled")
    page.close()
    return Image.open(output).convert("RGB")


def main() -> None:
    resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
    server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_port}/widget/render.html"
    browser_path = os.environ.get("CHROME_BIN")
    scratch_square = ROOT / "widget" / ".widget-square-render.png"
    scratch_medium = ROOT / "widget" / ".widget-medium-render.png"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, executable_path=browser_path or None, args=["--no-sandbox"])
            square = render_page(browser, origin, PREVIEW_SIZE, scratch_square)
            square.save(PREVIEW, format="PNG", optimize=True)
            small_image = square.resize(SMALL_SIZE, resampling)
            save_jpeg(small_image, SMALL)

            # The panorama keeps the same 78E–148E span as the China frame; its
            # north edge is Urumqi (~43.8N), with the south edge fitted to 360:169.
            medium_image = render_page(browser, origin + "?size=medium", MEDIUM_SIZE, scratch_medium)
            save_jpeg(medium_image, MEDIUM)

            # Fill the large-widget frame without adding black bars.
            # Center-crop the square map to the target aspect ratio, then scale.
            crop_width = round(square.height * LARGE_SIZE[0] / LARGE_SIZE[1])
            left = (square.width - crop_width) // 2
            large_map = square.crop((left, 0, left + crop_width, square.height))
            large_canvas = large_map.resize(LARGE_SIZE, resampling)
            save_jpeg(large_canvas, LARGE)
            browser.close()
    finally:
        server.shutdown()
        server.server_close()
        scratch_square.unlink(missing_ok=True)
        scratch_medium.unlink(missing_ok=True)

    expected = {PREVIEW: PREVIEW_SIZE, SMALL: SMALL_SIZE, MEDIUM: MEDIUM_SIZE, LARGE: LARGE_SIZE}
    for path, size in expected.items():
        if not path.exists() or path.stat().st_size < 10_000:
            raise RuntimeError(f"Screenshot missing or unexpectedly small: {path}")
        with Image.open(path) as image:
            if image.size != size:
                raise RuntimeError(f"Expected {size} image at {path}, got {image.size}")
        if path.stat().st_size >= 180_000:
            raise RuntimeError(f"Widget JPEG must stay below the persistent-cache budget: {path} ({path.stat().st_size} bytes)")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "image": PREVIEW.name,
        "width": PREVIEW_SIZE[0],
        "height": PREVIEW_SIZE[1],
        "widget_images": {
            "small": {"image": SMALL.name, "width": SMALL_SIZE[0], "height": SMALL_SIZE[1], "scale": 3},
            "medium": {"image": MEDIUM.name, "width": MEDIUM_SIZE[0], "height": MEDIUM_SIZE[1], "scale": 3, "north_edge_lat": 43.8},
            "large": {"image": LARGE.name, "width": LARGE_SIZE[0], "height": LARGE_SIZE[1], "scale": 3},
        },
    }
    (ROOT / "widget" / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {PREVIEW.relative_to(ROOT)} ({PREVIEW.stat().st_size} bytes)")
    for path in (SMALL, MEDIUM, LARGE):
        print(f"Generated {path.name}: {Image.open(path).size}, {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()

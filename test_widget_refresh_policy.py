#!/usr/bin/env python3
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parent
render = (ROOT / "widget" / "render.html").read_text(encoding="utf-8")
generator = (ROOT / "scripts" / "generate_widget_image.py").read_text(encoding="utf-8")
workflow = (ROOT / ".github" / "workflows" / "widget_map.yml").read_text(encoding="utf-8")

assert "launchMarkerOffsets" in render
assert "permanent:true" in render
assert "if(!isUnitedStates)" in render
assert "../upcoming_launches.json?widget=" in render
assert "../upcoming_launches.csv?widget=" in render
assert "Starlink Group 15-15" not in render  # source data remains dynamic
assert "make_scriptwidget_package" not in generator
assert 'ZipFile(output, "w"' not in generator
assert ".swt" not in workflow.split("file_pattern:", 1)[1].splitlines()[0]

expected_images = {
    "us-widget-small.jpg": (507, 507),
    "us-widget-medium.jpg": (1080, 507),
    "us-widget-large.jpg": (1092, 1146),
}
from PIL import Image
for name, size in expected_images.items():
    with Image.open(ROOT / "widget" / name) as image:
        assert image.size == size, (name, image.size)

for name in ("small", "medium", "large"):
    package = ROOT / "widget" / f"us-rocket-map-{name}.swt"
    with zipfile.ZipFile(package) as archive:
        assert set(archive.namelist()) == {"widget.json", "main.jsx"}

print("test_widget_refresh_policy.py passed")

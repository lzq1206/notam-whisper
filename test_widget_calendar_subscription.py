#!/usr/bin/env python3
from pathlib import Path

html = (Path(__file__).resolve().parent / "widget" / "index.html").read_text(encoding="utf-8")
section_start = html.index('id="calendar-subscription"')
section_end = html.index('</section>', section_start)
section = html[section_start:section_end]

FEED = "https://ll.thespacedevs.com/launches/latest/feed.ics"
assert FEED in section
assert '不要直接点击打开' in section
assert '不要直接点开上面的 ICS 地址' in section
assert '通过 URL 添加' in section
assert '添加已订阅的日历' in section
assert '不要选择“导入”本地 `.ics` 文件' in section
assert f'href="{FEED}"' not in section
assert section_start > html.index('id="usMapImage"')

print("test_widget_calendar_subscription.py passed")

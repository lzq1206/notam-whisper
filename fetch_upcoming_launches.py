#!/usr/bin/env python3
"""Fetch a bounded upcoming-launch snapshot for the static website."""
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import requests

API_URL = "https://ll.thespacedevs.com/2.3.0/launches/upcoming/"
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "upcoming_launches.json")
WINDOW_DAYS = 60
PAGE_LIMIT = 3
PAGE_SIZE = 100
REQUEST_TIMEOUT = 25
USER_AGENT = "notam-whisper/1.0 (+https://github.com/lzq1206/notam-whisper)"


def utc_iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def fetch_upcoming_snapshot(now=None):
    now = now or datetime.now(timezone.utc)
    start = now.astimezone(timezone.utc)
    end = start + timedelta(days=WINDOW_DAYS)
    params = {
        "limit": PAGE_SIZE,
        "mode": "normal",
        "ordering": "window_start",
        "window_end__gte": utc_iso(start),
        "window_start__lte": utc_iso(end),
    }
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    results = []
    seen = set()
    next_url = API_URL

    for page in range(PAGE_LIMIT):
        response = requests.get(
            next_url, params=params if page == 0 else None, headers=headers, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        payload = response.json()
        page_results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(page_results, list):
            raise ValueError("Launch Library response did not contain a results list")
        for launch in page_results:
            if not isinstance(launch, dict):
                continue
            key = launch.get("id") or launch.get("url") or launch.get("slug")
            if key is None or key in seen:
                continue
            seen.add(key)
            results.append(launch)
        next_url = payload.get("next") if isinstance(payload, dict) else None
        if not next_url:
            break

    if not results:
        raise ValueError("Launch Library returned no upcoming launches; retaining last good snapshot")

    return {
        "generated_at": utc_iso(datetime.now(timezone.utc)),
        "source": "The Space Devs Launch Library 2",
        "window_start": utc_iso(start),
        "window_end": utc_iso(end),
        "window_days": WINDOW_DAYS,
        "item_count": len(results),
        "results": results,
    }


def write_snapshot(snapshot):
    directory = os.path.dirname(OUTPUT_PATH)
    fd, temporary_path = tempfile.mkstemp(prefix=".upcoming_launches.", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
        os.replace(temporary_path, OUTPUT_PATH)
    finally:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)


if __name__ == "__main__":
    snapshot = fetch_upcoming_snapshot()
    write_snapshot(snapshot)
    print(f"Saved {snapshot['item_count']} upcoming launches covering {snapshot['window_days']} days")

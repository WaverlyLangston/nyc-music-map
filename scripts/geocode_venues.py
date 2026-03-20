#!/usr/bin/env python3
"""
geocode_venues.py — One-time utility to add/verify lat/lng coordinates.

Usage:
    MAPBOX_TOKEN=pk.xxx python scripts/geocode_venues.py

Only geocodes venues missing lat or lng. Safe to re-run.
"""

import json
import os
import time
import logging
from pathlib import Path
import requests

BASE_DIR    = Path(__file__).resolve().parent.parent
VENUES_FILE = BASE_DIR / "venues.json"
TOKEN       = os.environ.get("MAPBOX_TOKEN","")

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def geocode(address):
    if not TOKEN:
        raise RuntimeError("Set MAPBOX_TOKEN env var")
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{requests.utils.quote(address)}.json"
    r   = requests.get(url, params={
        "access_token": TOKEN,
        "country":      "US",
        "bbox":         "-74.3,40.45,-73.65,40.92",
        "limit":        "1",
    }, timeout=10)
    r.raise_for_status()
    feats = r.json().get("features",[])
    if not feats:
        return None
    lng, lat = feats[0]["center"]
    return round(lat,6), round(lng,6)


def main():
    if not TOKEN:
        log.error("Set MAPBOX_TOKEN env var before running.")
        raise SystemExit(1)
    venues  = json.loads(VENUES_FILE.read_text())
    updated = 0
    for i, v in enumerate(venues, 1):
        if v.get("lat") and v.get("lng"):
            continue
        log.info("[%d/%d] Geocoding: %s", i, len(venues), v["name"])
        try:
            result = geocode(v["address"])
            if result:
                v["lat"], v["lng"] = result
                updated += 1
                log.info("  → %.6f, %.6f", v["lat"], v["lng"])
            else:
                log.warning("  → no result")
        except Exception as exc:
            log.warning("  → error: %s", exc)
        time.sleep(0.3)
    VENUES_FILE.write_text(json.dumps(venues, indent=2, ensure_ascii=False))
    log.info("Done. Updated %d venue(s).", updated)


if __name__ == "__main__":
    main()

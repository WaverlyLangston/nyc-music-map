#!/usr/bin/env python3
"""
geocode_venues.py
-----------------
ONE-TIME utility: verifies / updates lat/lng coordinates for all venues
using the Mapbox Geocoding API. Reads venues.json, adds or corrects
coordinates, writes the result back.

Usage:
    MAPBOX_TOKEN=pk.xxx python scripts/geocode_venues.py

Only run this when you add new venues or want to re-verify coordinates.
The result is committed to the repo so the website never needs to geocode
at runtime.
"""

import json
import os
import time
import logging
from pathlib import Path

import requests

BASE_DIR    = Path(__file__).resolve().parent.parent
VENUES_FILE = BASE_DIR / "venues.json"
MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN", "")

GEOCODE_URL = "https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"
DELAY = 0.3   # seconds between API calls

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def geocode(address: str) -> tuple[float, float] | None:
    """Return (lat, lng) for an address via Mapbox, or None on failure."""
    if not MAPBOX_TOKEN:
        raise RuntimeError("MAPBOX_TOKEN env var not set")
    url = GEOCODE_URL.format(query=requests.utils.quote(address))
    params = {
        "access_token": MAPBOX_TOKEN,
        "country": "US",
        "bbox": "-74.3,40.45,-73.65,40.92",  # NYC bounding box
        "limit": 1,
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    features = r.json().get("features", [])
    if not features:
        return None
    lng, lat = features[0]["center"]
    return round(lat, 6), round(lng, 6)


def main():
    if not MAPBOX_TOKEN:
        log.error("Set MAPBOX_TOKEN env var before running.")
        raise SystemExit(1)

    venues = json.loads(VENUES_FILE.read_text())
    updated = 0

    for i, venue in enumerate(venues, 1):
        address = venue.get("address", "")
        existing_lat = venue.get("lat")
        existing_lng = venue.get("lng")

        log.info("[%d/%d] %s", i, len(venues), venue["name"])

        # Skip if coordinates already exist
        if existing_lat and existing_lng:
            log.info("  already has coords (%.4f, %.4f) — skipping",
                     existing_lat, existing_lng)
            time.sleep(DELAY)
            continue

        try:
            result = geocode(address)
            if result:
                lat, lng = result
                venue["lat"] = lat
                venue["lng"] = lng
                updated += 1
                log.info("  geocoded → (%.6f, %.6f)", lat, lng)
            else:
                log.warning("  no result for: %s", address)
        except Exception as exc:
            log.warning("  geocode error: %s", exc)

        time.sleep(DELAY)

    VENUES_FILE.write_text(json.dumps(venues, indent=2, ensure_ascii=False))
    log.info("Done. Updated %d venue(s). Written to %s", updated, VENUES_FILE)


if __name__ == "__main__":
    main()

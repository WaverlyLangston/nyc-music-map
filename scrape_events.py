#!/usr/bin/env python3
"""
scrape_events.py
----------------
Scrapes today's music events from all NYC venue calendar pages.
Outputs events.json consumed by the map website.

Run daily via GitHub Actions at 7am ET.
"""

import json
import re
import time
import logging
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ── Configuration ──────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).resolve().parent.parent
VENUES_FILE = BASE_DIR / "venues.json"
EVENTS_FILE = BASE_DIR / "events.json"

NYC_TZ  = ZoneInfo("America/New_York")
TODAY   = date.today()           # naive date in UTC; GH Actions UTC == correct day
TODAY_STR = TODAY.strftime("%-m/%-d/%Y")   # e.g. "3/19/2026"
TODAY_STR_SLASH = TODAY.strftime("%m/%d")  # e.g. "03/19"
TODAY_ISO = TODAY.isoformat()              # e.g. "2026-03-19"
TODAY_LONG = TODAY.strftime("%B %-d, %Y") # e.g. "March 19, 2026"
TODAY_LONG2 = TODAY.strftime("%A, %B %-d") # e.g. "Wednesday, March 19"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 15   # seconds
DELAY_BETWEEN   = 1.0  # seconds between venue fetches (be polite)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

def fetch_html(url: str) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup object, or None on failure."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as exc:
        log.warning("  fetch failed for %s — %s", url, exc)
        return None


def today_in_text(text: str) -> bool:
    """Return True if any recognisable form of today's date appears in text."""
    text = text.lower()
    patterns = [
        TODAY_ISO,
        TODAY_STR.lower(),
        TODAY_STR_SLASH,
        TODAY_LONG.lower(),
        TODAY_LONG2.lower(),
        TODAY.strftime("%b %-d").lower(),    # Mar 19
        TODAY.strftime("%B %-d").lower(),    # March 19
        TODAY.strftime("%d %B").lower(),     # 19 March
    ]
    return any(p in text for p in patterns)


def clean(text: str) -> str:
    return " ".join(text.split())


def parse_time(text: str) -> str:
    """Pull the first time-like token from a string."""
    m = re.search(r'\d{1,2}(?::\d{2})?\s*(?:am|pm)', text, re.I)
    return m.group(0).upper() if m else ""


def parse_price(text: str) -> str:
    """Pull the first dollar amount from a string."""
    m = re.search(r'\$\s*[\d,]+(?:\.\d{2})?', text)
    if m:
        return m.group(0)
    if re.search(r'\bfree\b', text, re.I):
        return "Free"
    return ""


# ── Platform-specific parsers ──────────────────────────────────────────────────

def parse_eventbrite_widget(soup: BeautifulSoup) -> list[dict]:
    """Many venues embed Eventbrite. Look for structured-data JSON."""
    events = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("Event", "MusicEvent"):
                    continue
                start_raw = item.get("startDate", "")
                if not start_raw:
                    continue
                # Check date
                if TODAY_ISO not in start_raw:
                    continue
                name = clean(item.get("name", ""))
                if not name:
                    continue
                # start time
                try:
                    dt = datetime.fromisoformat(start_raw)
                    start_time = dt.strftime("%-I:%M %p")
                except Exception:
                    start_time = parse_time(start_raw)
                # price
                offers = item.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                price = ""
                if isinstance(offers, dict):
                    low = offers.get("lowPrice", "")
                    high = offers.get("highPrice", "")
                    avail = offers.get("availability", "")
                    if "free" in str(avail).lower() or (str(low) == "0"):
                        price = "Free"
                    elif low:
                        price = f"${low}" + (f"–${high}" if high and high != low else "")
                performers = []
                for p in item.get("performer", []):
                    if isinstance(p, dict):
                        performers.append(clean(p.get("name", "")))
                    elif isinstance(p, str):
                        performers.append(clean(p))
                events.append({
                    "name": name,
                    "start_time": start_time,
                    "ticket_price": price,
                    "performers": performers,
                })
        except Exception:
            pass
    return events


def parse_generic(soup: BeautifulSoup, venue_name: str) -> list[dict]:
    """
    Generic heuristic scraper. Looks for event blocks that contain today's date.
    Tries common HTML patterns used by Squarespace, WordPress + The Events Calendar,
    custom venue sites, etc.
    """
    events = []

    # ── Strategy 1: JSON-LD structured data (schema.org Event) ────────────────
    ld_events = parse_eventbrite_widget(soup)
    if ld_events:
        return ld_events

    # ── Strategy 2: WordPress "The Events Calendar" markup ────────────────────
    for article in soup.find_all("article", class_=re.compile(r"type-tribe_events|type-events|tribe")):
        text = article.get_text(" ", strip=True)
        if not today_in_text(text):
            continue
        title_el = (
            article.find(class_=re.compile(r"tribe-event-name|entry-title|event-title")) or
            article.find(["h1","h2","h3"])
        )
        name = clean(title_el.get_text()) if title_el else ""
        if not name:
            continue
        time_el = article.find(class_=re.compile(r"tribe-event-time|time|starts"))
        start_time = parse_time(time_el.get_text() if time_el else text)
        price_el = article.find(class_=re.compile(r"tribe-ticket|cost|price"))
        price = parse_price(price_el.get_text() if price_el else text)
        events.append({"name": name, "start_time": start_time,
                        "ticket_price": price, "performers": []})

    if events:
        return events

    # ── Strategy 3: Squarespace / generic "event" class containers ────────────
    candidates = soup.find_all(class_=re.compile(
        r"event(?!brite)|show|gig|performance|listing|calendar-event|sqs-block-event",
        re.I
    ))
    for el in candidates:
        text = el.get_text(" ", strip=True)
        if not today_in_text(text):
            continue
        # must have a reasonable amount of text
        if len(text) < 10:
            continue
        heading = (
            el.find(["h1","h2","h3","h4","strong"]) or
            el.find(class_=re.compile(r"title|name", re.I))
        )
        name = clean(heading.get_text()) if heading else clean(text[:80])
        start_time = parse_time(text)
        price = parse_price(text)
        events.append({"name": name, "start_time": start_time,
                        "ticket_price": price, "performers": []})

    if events:
        return events

    # ── Strategy 4: Scan ALL text blocks for date → grab nearby heading ───────
    all_tags = soup.find_all(["p","li","div","td","span"])
    for tag in all_tags:
        own_text = tag.get_text(" ", strip=True)
        if today_in_text(own_text) and len(own_text) < 200:
            # walk up to find a meaningful heading sibling or parent
            parent = tag.parent
            heading = parent.find(["h1","h2","h3","h4"]) if parent else None
            name = clean(heading.get_text()) if heading else clean(own_text[:80])
            start_time = parse_time(own_text)
            price = parse_price(own_text)
            if name and name not in [e["name"] for e in events]:
                events.append({"name": name, "start_time": start_time,
                                "ticket_price": price, "performers": []})

    return events


# ── Platform routers ───────────────────────────────────────────────────────────

def scrape_resident_advisor(venue_id: str) -> list[dict]:
    """Pull today's events from a Resident Advisor venue page."""
    url = f"https://ra.co/clubs/{venue_id}"
    soup = fetch_html(url)
    if not soup:
        return []
    return parse_generic(soup, venue_id)


def scrape_dice(venue_slug: str) -> list[dict]:
    """DICE venue page — relies on JSON-LD."""
    url = f"https://dice.fm/venue/{venue_slug}"
    soup = fetch_html(url)
    if not soup:
        return []
    return parse_eventbrite_widget(soup) or parse_generic(soup, venue_slug)


def scrape_venue(venue: dict) -> list[dict]:
    """Route to the right scraper and return a (possibly empty) events list."""
    url = venue.get("calendar_url") or venue.get("website")
    if not url:
        return []

    # ── Platform detection ─────────────────────────────────────────────────────
    if "ra.co/clubs/" in url:
        venue_id = url.rstrip("/").split("/")[-1]
        return scrape_resident_advisor(venue_id)
    if "dice.fm/venue/" in url:
        slug = url.rstrip("/").split("/")[-1]
        return scrape_dice(slug)

    # ── Generic fetch ──────────────────────────────────────────────────────────
    soup = fetch_html(url)
    if not soup:
        return []
    return parse_generic(soup, venue.get("name", url))


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("=== NYC Music Map — Event Scraper ===")
    log.info("Date: %s", TODAY_ISO)

    venues = json.loads(VENUES_FILE.read_text())
    log.info("Loaded %d venues", len(venues))

    results: dict[str, list] = {}
    success_count = 0
    event_count   = 0

    for i, venue in enumerate(venues, 1):
        vid  = str(venue["id"])
        name = venue["name"]
        log.info("[%d/%d] %s", i, len(venues), name)

        try:
            events = scrape_venue(venue)
        except Exception as exc:
            log.warning("  !! unhandled error: %s", exc)
            events = []

        if events:
            results[vid] = events
            success_count += 1
            event_count   += len(events)
            log.info("  → %d event(s) found", len(events))
        else:
            log.info("  → no events today")

        # Polite delay
        time.sleep(DELAY_BETWEEN)

    output = {
        "generated_at": datetime.now(NYC_TZ).isoformat(),
        "date": TODAY_ISO,
        "events": results,
    }

    EVENTS_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    log.info("=== Done — %d venues with events, %d total events ===",
             success_count, event_count)
    log.info("Written to %s", EVENTS_FILE)


if __name__ == "__main__":
    main()

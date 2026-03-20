#!/usr/bin/env python3
"""
scrape_events.py
----------------
Scrapes today's music events from NYC venue calendar pages.
Outputs events.json — only venues with confirmed shows appear.

Run daily at 7am ET via GitHub Actions.
"""

import json, re, time, logging
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent.parent
VENUES_FILE = BASE_DIR / "venues.json"
EVENTS_FILE = BASE_DIR / "events.json"

# ── Date strings used for matching ────────────────────────────────────────
NYC_TZ    = ZoneInfo("America/New_York")
TODAY     = date.today()
TODAY_ISO = TODAY.isoformat()                      # 2026-03-19
TODAY_STR = TODAY.strftime("%-m/%-d/%Y")           # 3/19/2026
TODAY_MDY = TODAY.strftime("%m/%d/%Y")             # 03/19/2026
TODAY_MD  = TODAY.strftime("%-m/%-d")              # 3/19
TODAY_LONG      = TODAY.strftime("%B %-d, %Y")     # March 19, 2026
TODAY_LONG_SHORT = TODAY.strftime("%b %-d, %Y")    # Mar 19, 2026
TODAY_LONG2     = TODAY.strftime("%B %-d")         # March 19
TODAY_SHORT2    = TODAY.strftime("%b %-d")         # Mar 19
TODAY_DOW       = TODAY.strftime("%A, %B %-d")     # Thursday, March 19

DATE_NEEDLES = [
    TODAY_ISO, TODAY_STR, TODAY_MDY, TODAY_MD,
    TODAY_LONG.lower(), TODAY_LONG_SHORT.lower(),
    TODAY_LONG2.lower(), TODAY_SHORT2.lower(),
    TODAY_DOW.lower(),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 15
DELAY   = 1.2   # seconds between requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Utilities ──────────────────────────────────────────────────────────────

def fetch(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except Exception as exc:
        log.warning("  fetch failed: %s", exc)
        return None


def clean(t: str) -> str:
    return " ".join(t.split()).strip()


def has_today(text: str) -> bool:
    t = text.lower()
    return any(needle in t for needle in DATE_NEEDLES)


def extract_time(text: str) -> str:
    """Return first HH:MM am/pm pattern found, or empty string."""
    m = re.search(r'\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b', text, re.I)
    return m.group(1).strip().upper() if m else ""


def extract_price(text: str) -> str:
    """Return ticket price string or empty."""
    if re.search(r'\bfree\b', text, re.I):
        return "Free"
    m = re.search(r'\$\s?(\d+(?:\.\d{2})?)', text)
    return f"${m.group(1)}" if m else ""


def make_event(name: str, start_time: str = "", price: str = "",
               performers: list[str] | None = None) -> dict:
    return {
        "name":         clean(name),
        "start_time":   start_time,
        "ticket_price": price,
        "performers":   performers or [],
    }


# ── Schema.org JSON-LD parser (most reliable source) ──────────────────────

def parse_jsonld(soup: BeautifulSoup) -> list[dict]:
    results = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            raw = tag.string or ""
            data = json.loads(raw)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") not in ("Event", "MusicEvent", "Concert"):
                    continue
                start = item.get("startDate", "")
                if not start or TODAY_ISO not in start:
                    continue
                name = clean(item.get("name", ""))
                if not name:
                    continue
                # Parse start time
                try:
                    dt = datetime.fromisoformat(start)
                    t  = dt.strftime("%-I:%M %p")
                except Exception:
                    t = extract_time(start)
                # Parse price
                offers = item.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                price = ""
                if isinstance(offers, dict):
                    low   = str(offers.get("lowPrice",  ""))
                    high  = str(offers.get("highPrice", ""))
                    avail = str(offers.get("availability", ""))
                    if low == "0" or "free" in avail.lower():
                        price = "Free"
                    elif low:
                        price = f"${low}" + (f"–${high}" if high and high != low else "")
                # Parse performers
                perfs = []
                for p in item.get("performer", []):
                    n = p.get("name", "") if isinstance(p, dict) else str(p)
                    if n:
                        perfs.append(clean(n))
                results.append(make_event(name, t, price, perfs))
        except Exception:
            pass
    return results


# ── WordPress "The Events Calendar" parser ────────────────────────────────

def parse_tribe(soup: BeautifulSoup) -> list[dict]:
    results = []
    for article in soup.find_all("article", class_=re.compile(r"tribe", re.I)):
        text = article.get_text(" ", strip=True)
        if not has_today(text):
            continue
        title = (
            article.find(class_=re.compile(r"tribe-event-name|entry-title", re.I))
            or article.find(["h1","h2","h3","h4"])
        )
        if not title:
            continue
        name = clean(title.get_text())
        if not name:
            continue
        time_el  = article.find(class_=re.compile(r"tribe-event-time|tribe-events-start-time", re.I))
        price_el = article.find(class_=re.compile(r"tribe-ticket|tribe-cost", re.I))
        results.append(make_event(
            name,
            extract_time(time_el.get_text() if time_el else text),
            extract_price(price_el.get_text() if price_el else text),
        ))
    return results


# ── Generic CSS-class heuristic parser ────────────────────────────────────

def parse_generic(soup: BeautifulSoup) -> list[dict]:
    results = []
    seen_names = set()

    # Look for elements whose class suggests "event" and whose text contains today's date
    candidates = soup.find_all(
        True,
        class_=re.compile(
            r'\b(event|show|gig|performance|listing|concert|calendar[-_]?item)\b',
            re.I
        )
    )
    for el in candidates:
        text = el.get_text(" ", strip=True)
        if not has_today(text):
            continue
        if len(text) < 8:
            continue

        heading = (
            el.find(["h1","h2","h3","h4","strong"])
            or el.find(class_=re.compile(r"title|name|heading", re.I))
        )
        name = clean(heading.get_text()) if heading else clean(text[:80])
        if not name or name in seen_names:
            continue
        seen_names.add(name)

        results.append(make_event(
            name,
            extract_time(text),
            extract_price(text),
        ))

    return results


# ── Date-proximity text scanner (last resort) ─────────────────────────────

def parse_text_scan(soup: BeautifulSoup) -> list[dict]:
    """
    Walk every text node. When we find today's date, look at surrounding
    siblings/parent for an event name, time, and price.
    """
    results = []
    seen = set()
    body = soup.find("body")
    if not body:
        return results

    for el in body.find_all(["li","tr","div","article","section"]):
        text = el.get_text(" ", strip=True)
        if not has_today(text):
            continue
        if len(text) > 600:   # skip huge containers
            continue

        heading = el.find(["h1","h2","h3","h4","strong","b","a"])
        name = clean(heading.get_text()) if heading else clean(text[:100])
        if not name or name in seen or len(name) < 4:
            continue
        seen.add(name)

        results.append(make_event(
            name,
            extract_time(text),
            extract_price(text),
        ))

    return results


# ── Master scraper per venue ───────────────────────────────────────────────

def scrape_venue(venue: dict) -> list[dict]:
    url = venue.get("calendar_url") or venue.get("website")
    if not url:
        return []

    soup = fetch(url)
    if not soup:
        return []

    # Try parsers in order of reliability
    for parser in [parse_jsonld, parse_tribe, parse_generic, parse_text_scan]:
        results = parser(soup)
        if results:
            log.info("  → %d event(s) via %s", len(results), parser.__name__)
            return results

    return []


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    log.info("=== NYC Music Map Event Scraper  date=%s ===", TODAY_ISO)
    venues = json.loads(VENUES_FILE.read_text())
    log.info("Loaded %d venues", len(venues))

    events_out: dict[str, list] = {}
    found_count = 0

    for i, venue in enumerate(venues, 1):
        vid  = str(venue["id"])
        name = venue["name"]
        log.info("[%d/%d] %s", i, len(venues), name)
        try:
            evs = scrape_venue(venue)
        except Exception as exc:
            log.warning("  !! error: %s", exc)
            evs = []

        if evs:
            events_out[vid] = evs
            found_count += 1

        time.sleep(DELAY)

    output = {
        "generated_at": datetime.now(NYC_TZ).isoformat(),
        "date":         TODAY_ISO,
        "events":       events_out,
    }
    EVENTS_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    log.info("=== Done — %d/%d venues have events today ===", found_count, len(venues))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
scrape_events.py
----------------
Daily scraper for NYC Music Map. Runs every morning via GitHub Actions.

For each venue:
  1. Fetches the calendar page (headless Chromium)
  2. Extracts today's shows: name, time, price, performers
  3. For each performer, grabs any artist info visible on the page:
       - bio / description text
       - website link
       - social media links (Instagram, Bandcamp, Spotify, etc.)
  4. Stores all of this in artists.db
  5. Writes events.json with fully populated performer data

enrich_artists.py then fills in whatever is still missing from
MusicBrainz and Last.fm.

Target runtime: 10-20 minutes. No external API calls.
"""

import asyncio
import json
import logging
import re
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup, Tag
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

BASE_DIR    = Path(__file__).resolve().parent.parent
VENUES_FILE = BASE_DIR / "venues.json"
EVENTS_FILE = BASE_DIR / "events.json"
DB_FILE     = BASE_DIR / "artists.db"

NYC_TZ    = ZoneInfo("America/New_York")
TODAY     = date.today()
TODAY_ISO = TODAY.isoformat()

DATE_NEEDLES = [
    TODAY_ISO,
    TODAY.strftime("%B %-d, %Y").lower(),
    TODAY.strftime("%b %-d, %Y").lower(),
    TODAY.strftime("%B %-d").lower(),
    TODAY.strftime("%b %-d").lower(),
    TODAY.strftime("%-m/%-d/%Y"),
    TODAY.strftime("%m/%d/%Y"),
    TODAY.strftime("%-m/%-d"),
    TODAY.strftime("%m/%d"),
]

CONCURRENCY = 5
PAGE_MS     = 18000
SETTLE_MS   = 1800
REQ_DELAY   = 0.5

SOCIAL_FIELDS = [
    "instagram", "bandcamp", "facebook",
    "spotify", "twitter", "youtube", "soundcloud", "tiktok",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
# ARTIST DATABASE
# ══════════════════════════════════════════════════════════════════════════

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS artists (
            name_lower      TEXT PRIMARY KEY,
            name_display    TEXT NOT NULL,
            -- MusicBrainz identifier (set by enrich_artists.py)
            mbid            TEXT DEFAULT '',
            disambiguation  TEXT DEFAULT '',
            -- Bio/description: venue page takes priority over Last.fm
            description     TEXT DEFAULT '',
            description_src TEXT DEFAULT '',   -- 'venue', 'lastfm', 'manual'
            -- Website
            website         TEXT DEFAULT '',
            website_src     TEXT DEFAULT '',
            -- Social links
            instagram       TEXT DEFAULT '',
            instagram_src   TEXT DEFAULT '',
            bandcamp        TEXT DEFAULT '',
            bandcamp_src    TEXT DEFAULT '',
            facebook        TEXT DEFAULT '',
            facebook_src    TEXT DEFAULT '',
            spotify         TEXT DEFAULT '',
            spotify_src     TEXT DEFAULT '',
            twitter         TEXT DEFAULT '',
            twitter_src     TEXT DEFAULT '',
            youtube         TEXT DEFAULT '',
            youtube_src     TEXT DEFAULT '',
            soundcloud      TEXT DEFAULT '',
            soundcloud_src  TEXT DEFAULT '',
            tiktok          TEXT DEFAULT '',
            tiktok_src      TEXT DEFAULT '',
            -- Status
            enriched        INTEGER DEFAULT 0,  -- 1 = MB/Last.fm enrichment done
            enriched_at     TEXT DEFAULT '',
            first_seen      TEXT NOT NULL,
            last_seen       TEXT NOT NULL,
            seen_count      INTEGER DEFAULT 1,
            -- Set to 1 to lock this record from automated changes
            locked          INTEGER DEFAULT 0
        );

        -- URL fingerprints for same-name disambiguation
        CREATE TABLE IF NOT EXISTS artist_urls (
            url_normalized  TEXT NOT NULL,
            name_lower      TEXT NOT NULL REFERENCES artists(name_lower),
            url_type        TEXT DEFAULT 'unknown',
            PRIMARY KEY (url_normalized, name_lower)
        );

        CREATE INDEX IF NOT EXISTS idx_unenriched
            ON artists(enriched) WHERE enriched = 0;
    """)
    conn.commit()
    conn.close()


def normalize_url(url: str) -> str:
    url = url.strip().lower()
    try:
        p    = urlparse(url if "://" in url else "https://" + url)
        host = p.netloc.replace("www.", "").strip(".")
        path = p.path.rstrip("/")
        return f"{host}{path}" if host else ""
    except Exception:
        return ""


def classify_url(url: str) -> str:
    u = url.lower()
    if "instagram.com"    in u: return "instagram"
    if ".bandcamp.com"    in u: return "bandcamp"
    if "facebook.com"     in u: return "facebook"
    if "open.spotify.com" in u or "spotify.com/artist" in u: return "spotify"
    if "twitter.com"      in u or re.search(r'\bx\.com/', u): return "twitter"
    if "youtube.com"      in u or "youtu.be" in u: return "youtube"
    if "soundcloud.com"   in u: return "soundcloud"
    if "tiktok.com"       in u: return "tiktok"
    return "website"


def upsert_artist(name: str, scraped: dict):
    """
    Insert or update an artist record.

    scraped = {
        "description": str,
        "website": str,
        "instagram": str,
        "bandcamp": str,
        ... (any social field)
    }

    Rules:
    - Locked records are never modified.
    - Venue-scraped data ('venue' source) beats automated enrichment
      ('lastfm', 'musicbrainz') but never overwrites another venue scrape.
    - seen_count and last_seen are always updated.
    """
    nl  = name.lower().strip()
    now = datetime.utcnow().isoformat()

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    existing = conn.execute(
        "SELECT * FROM artists WHERE name_lower = ?", (nl,)
    ).fetchone()

    if not existing:
        # New artist — insert everything we have
        conn.execute(
            """INSERT INTO artists
               (name_lower, name_display,
                description, description_src,
                website, website_src,
                instagram, instagram_src,
                bandcamp, bandcamp_src,
                facebook, facebook_src,
                spotify, spotify_src,
                twitter, twitter_src,
                youtube, youtube_src,
                soundcloud, soundcloud_src,
                tiktok, tiktok_src,
                first_seen, last_seen, seen_count, enriched)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)""",
            (
                nl, name.strip(),
                scraped.get("description", ""),
                "venue" if scraped.get("description") else "",
                scraped.get("website", ""),
                "venue" if scraped.get("website") else "",
                scraped.get("instagram", ""),
                "venue" if scraped.get("instagram") else "",
                scraped.get("bandcamp", ""),
                "venue" if scraped.get("bandcamp") else "",
                scraped.get("facebook", ""),
                "venue" if scraped.get("facebook") else "",
                scraped.get("spotify", ""),
                "venue" if scraped.get("spotify") else "",
                scraped.get("twitter", ""),
                "venue" if scraped.get("twitter") else "",
                scraped.get("youtube", ""),
                "venue" if scraped.get("youtube") else "",
                scraped.get("soundcloud", ""),
                "venue" if scraped.get("soundcloud") else "",
                scraped.get("tiktok", ""),
                "venue" if scraped.get("tiktok") else "",
                now, now,
            )
        )
    else:
        ex = dict(existing)
        if ex.get("locked"):
            conn.execute(
                "UPDATE artists SET last_seen=?, seen_count=seen_count+1 WHERE name_lower=?",
                (now, nl)
            )
            conn.commit()
            conn.close()
            return

        # Update only empty fields, or fields where venue source beats prior source
        updates = ["last_seen = ?", "seen_count = seen_count + 1"]
        params  = [now]

        def should_update(field: str, new_val: str) -> bool:
            if not new_val:
                return False
            old_val = ex.get(field, "")
            old_src = ex.get(f"{field}_src", "")
            if not old_val:
                return True   # field is empty — always fill it
            # Venue data beats non-venue automated data
            if old_src not in ("venue", "manual"):
                return True
            return False      # already have venue or manual data — keep it

        for field in ["description", "website"] + SOCIAL_FIELDS:
            new_val = scraped.get(field, "")
            if should_update(field, new_val):
                updates.append(f"{field} = ?")
                updates.append(f"{field}_src = ?")
                params.extend([new_val, "venue"])

        params.append(nl)
        conn.execute(f"UPDATE artists SET {', '.join(updates)} WHERE name_lower = ?", params)

    # Store URL fingerprints
    for field in ["website"] + SOCIAL_FIELDS:
        url = scraped.get(field, "")
        if url:
            norm = normalize_url(url)
            if norm:
                conn.execute(
                    "INSERT OR IGNORE INTO artist_urls (url_normalized, name_lower, url_type) VALUES (?,?,?)",
                    (norm, nl, field)
                )

    conn.commit()
    conn.close()


def read_artist(name: str) -> dict:
    """Return stored artist data for embedding in events.json."""
    nl   = name.lower().strip()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    row  = conn.execute("SELECT * FROM artists WHERE name_lower = ?", (nl,)).fetchone()
    conn.close()
    if not row:
        return _bare(name)
    r = dict(row)
    return {
        "name":        r["name_display"],
        "mbid":        r.get("mbid", ""),
        "description": r.get("description", ""),
        "website":     r.get("website", ""),
        "social": {f: r.get(f, "") for f in SOCIAL_FIELDS},
    }


def _bare(name: str) -> dict:
    return {
        "name": name, "mbid": "", "description": "", "website": "",
        "social": {f: "" for f in SOCIAL_FIELDS},
    }


# ══════════════════════════════════════════════════════════════════════════
# PERFORMER NAME FILTER
# ══════════════════════════════════════════════════════════════════════════

UI_WORDS = {
    "google calendar", "ics", "ics file", "add to calendar", "export",
    "subscribe", "view event", "rsvp", "buy tickets", "get tickets",
    "tickets", "read more", "learn more", "more info", "more details",
    "skip to content", "back", "menu", "news", "blog", "contact",
    "about", "staff", "board", "donate", "membership", "calendar",
    "events", "shows", "overview", "history", "press", "home",
    "photos", "videos", "gallery", "shop", "store", "merch",
    "folder: about", "folder: performing arts", "folder: visual arts",
    "folder: education", "folder: support", "folder: booking",
    "cocktails", "wine", "snacks", "beer", "food", "drinks",
    "drinks & foods", "jazz", "comedy", "music", "live", "free",
    "sold out", "our work", "or this work", "this page",
    "report a map error", "terms", "your impact", "corporate support",
    "deia plan", "making moves dance festival", "meet the playwright",
    "riddim section", "riddim and jazz festival",
    "a weekend of west african dance", "strength courage & wisdom",
    "current exhibitions", "visual voices", "open call exhibitions",
    "jamaica flux u", "artworks", "classes", "casa / su-casa",
    "queens international children's festival", "event calendar",
    "overview and history", "overview &", "artist registry",
    "join mailing list", "shoots + special events",
    "party & event booking", "halupka studio", "iykyk", "dnrnb",
    "order", "view event →", "rsvp ←", "tickets & tables ←",
    "tickets ←", "tickets here ←", "view us on eventbrite",
}

REJECT_RE = re.compile(
    r'^(\$[\d,]+|free|\d+\+|21\+|18\+|all ages)$'
    r'|[→←↗↙↑↓]'
    r'|@[a-z]'
    r'|\.(com|org|net|io|nyc)\b'
    r'|^\d{1,2}:\d{2}\s*(am|pm)$'
    r'|^(mon|tue|wed|thu|fri|sat|sun),?\s+\w+\s+\d+$'
    r'|^(january|february|march|april|may|june|july|august'
    r'|september|october|november|december)\s+\d+$',
    re.I
)
DATE_FRAG_RE = re.compile(
    r'^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{1,2}$', re.I
)
TIME_PREFIX_RE = re.compile(
    r'^\d{1,2}:\d{2}\s*(?:am|pm)\s+\d{2}:\d{2}\s+\d{1,2}:\d{2}\s*(?:am|pm)\s+(.+)$',
    re.I
)
BAD_PREFIXES = (
    "google ", "ics ", "folder:",
    "fri happy hour", "thurs happy hour", "sat 6-7",
    "tues 8-11", "fri 8 &", "sat 8 &", "thurs 8 &", "wed 8 &",
)


def is_valid_performer(name: str) -> bool:
    if not name or not name.strip():
        return False
    n  = name.strip()
    nl = n.lower()
    if len(n) < 2 or len(n) > 120:
        return False
    if nl in UI_WORDS:
        return False
    if DATE_FRAG_RE.match(nl):
        return False
    for prefix in BAD_PREFIXES:
        if nl.startswith(prefix):
            return False
    if REJECT_RE.search(n):
        return False
    if re.search(r'\d{1,2}:\d{2}\s*(am|pm)', n, re.I) and len(n) > 25:
        return False
    return True


def clean_performer_name(name: str) -> str:
    m = TIME_PREFIX_RE.match(name)
    return m.group(1).strip() if m else name.strip()


# ══════════════════════════════════════════════════════════════════════════
# ARTIST INFO EXTRACTION FROM PAGE HTML
# ══════════════════════════════════════════════════════════════════════════

def extract_artist_info_from_block(el: Tag, page_url: str = "") -> dict:
    """
    Given an HTML element that likely represents an artist listing,
    extract whatever info is visible:
      - bio / description paragraph
      - website link
      - social media links

    Returns a dict with fields matching the artists table.
    """
    info = {f: "" for f in ["description", "website"] + SOCIAL_FIELDS}

    # ── Social and website links ─────────────────────────────────────────
    for a in el.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("mailto"):
            continue
        # Make relative URLs absolute
        if href.startswith("/") and page_url:
            href = urljoin(page_url, href)
        kind = classify_url(href)
        if kind in SOCIAL_FIELDS and not info[kind]:
            info[kind] = href
        elif kind == "website" and not info["website"]:
            # Only treat as artist website if it doesn't look like the
            # venue's own domain
            info["website"] = href

    # ── Description / bio text ───────────────────────────────────────────
    # Look for a paragraph or div that reads like a bio
    # (multiple words, sentence-like, not a heading)
    bio_candidates = []
    for tag in el.find_all(["p", "div", "span"], recursive=True):
        # Skip if this contains sub-elements that are headings
        if tag.find(["h1", "h2", "h3", "h4"]):
            continue
        text = " ".join(tag.get_text(" ", strip=True).split())
        # Bio heuristic: 20+ chars, looks like a sentence, not a UI label
        if (len(text) > 20 and " " in text
                and not text.lower() in UI_WORDS
                and not REJECT_RE.search(text)
                and not re.match(r'^[\$\d]', text)):
            bio_candidates.append(text)

    if bio_candidates:
        # Pick the longest candidate up to 500 chars
        best = max(bio_candidates, key=len)
        if len(best) > 20:
            info["description"] = best[:500]

    return info


def extract_social_from_jsonld_performer(p: dict) -> dict:
    """Extract info from a schema.org performer object."""
    info = {f: "" for f in ["description", "website"] + SOCIAL_FIELDS}
    url  = p.get("url", "") if isinstance(p, dict) else ""
    if url:
        kind = classify_url(url)
        if kind in SOCIAL_FIELDS:
            info[kind] = url
        elif kind == "website":
            info["website"] = url
    desc = p.get("description", "") if isinstance(p, dict) else ""
    if desc:
        info["description"] = str(desc)[:500]
    return info


# ══════════════════════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def clean(s: str) -> str:
    return " ".join(str(s).split()).strip()


def has_today(text: str) -> bool:
    return any(n in text.lower() for n in DATE_NEEDLES)


def extract_time(text: str) -> str:
    m = re.search(r'\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b', text, re.I)
    return m.group(1).strip().upper() if m else ""


def extract_prices(text: str) -> tuple[str, str]:
    t = text.lower()
    if re.search(r'\bfree\b', t):
        return "Free", "Free"
    ad = re.search(
        r'\$(\d+(?:\.\d{2})?)\s*(?:adv(?:ance)?)\s*[/,|]\s*\$(\d+(?:\.\d{2})?)\s*(?:door)',
        text, re.I)
    if ad:
        return f"${ad.group(1)}", f"${ad.group(2)}"
    door = re.search(r'\$(\d+(?:\.\d{2})?)\s*(?:at\s*the\s*)?door', text, re.I)
    if door:
        return "", f"${door.group(1)}"
    adv = re.search(r'\$(\d+(?:\.\d{2})?)\s*(?:adv(?:ance)?)', text, re.I)
    if adv:
        return f"${adv.group(1)}", ""
    rng = re.search(r'\$(\d+)\s*[-–]\s*\$(\d+)', text)
    if rng:
        return f"${rng.group(1)}", f"${rng.group(2)}"
    single = re.search(r'\$(\d+(?:\.\d{2})?)', text)
    if single:
        p = f"${single.group(1)}"
        return p, p
    return "", ""


def make_show(name="", start_time="", end_time="",
              advance="", door="", ticket_url="",
              raw_performers=None):
    """
    Build a show dict.
    raw_performers is a list of {"name": str, **artist_info_fields}.
    Each performer is upserted into artists.db and their current
    stored data is embedded in the show.
    """
    perf_objects = []
    for rp in (raw_performers or []):
        pname = rp.get("name", "")
        pname = clean_performer_name(pname)
        if not is_valid_performer(pname):
            continue
        # Merge any info scraped from the venue page
        scraped = {k: v for k, v in rp.items()
                   if k != "name" and v}
        upsert_artist(pname, scraped)
        perf_objects.append(read_artist(pname))

    return {
        "name":                 clean(name),
        "start_time":           start_time,
        "end_time":             end_time,
        "ticket_price_advance": advance,
        "ticket_price_door":    door,
        "ticket_url":           ticket_url,
        "performers":           perf_objects,
    }


# ══════════════════════════════════════════════════════════════════════════
# PARSERS
# ══════════════════════════════════════════════════════════════════════════

def parse_jsonld(soup: BeautifulSoup, page_url: str = "") -> list[dict]:
    shows = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data  = json.loads(tag.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") not in ("Event", "MusicEvent", "Concert"):
                    continue
                start = item.get("startDate", "")
                if not start or TODAY_ISO not in start:
                    continue
                name = clean(item.get("name", "Event"))
                if not name:
                    continue
                try:
                    st = datetime.fromisoformat(start).strftime("%-I:%M %p")
                except Exception:
                    st = extract_time(start)
                end_t = ""
                if item.get("endDate"):
                    try:
                        end_t = datetime.fromisoformat(item["endDate"]).strftime("%-I:%M %p")
                    except Exception:
                        pass
                offers = item.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                adv = door = tick = ""
                if isinstance(offers, dict):
                    low   = str(offers.get("lowPrice", ""))
                    high  = str(offers.get("highPrice", ""))
                    avail = str(offers.get("availability", ""))
                    tick  = offers.get("url", "")
                    if low == "0" or "free" in avail.lower():
                        adv = door = "Free"
                    elif low:
                        p = f"${low}" + (f"–${high}" if high and high != low else "")
                        adv = door = p

                # Performers — JSON-LD sometimes has URLs and descriptions
                raw_perfs = []
                for p in (item.get("performer") or []):
                    pname = clean(p.get("name", "") if isinstance(p, dict) else str(p))
                    if not is_valid_performer(pname):
                        continue
                    info = extract_social_from_jsonld_performer(p)
                    raw_perfs.append({"name": pname, **info})

                shows.append(make_show(name, st, end_t, adv, door, tick, raw_perfs))
        except Exception:
            pass
    return shows


def parse_tribe(soup: BeautifulSoup, page_url: str = "") -> list[dict]:
    shows = []
    for art in soup.find_all("article", class_=re.compile(r"tribe", re.I)):
        text = art.get_text(" ", strip=True)
        if not has_today(text):
            continue
        title = (
            art.find(class_=re.compile(r"tribe-event-name|entry-title", re.I))
            or art.find(["h1", "h2", "h3", "h4"])
        )
        if not title:
            continue
        name = clean(title.get_text())
        if not name:
            continue
        tel  = art.find(class_=re.compile(r"tribe-event-time|tribe-events-start-time", re.I))
        pel  = art.find(class_=re.compile(r"tribe-ticket|tribe-cost", re.I))
        turl = ""
        tl   = art.find("a", href=re.compile(r"ticket|register|buy", re.I))
        if tl:
            turl = tl.get("href", "")
        adv, door = extract_prices(pel.get_text() if pel else text)

        # Look for performer sub-blocks within the event article
        raw_perfs = []
        for perf_el in art.find_all(class_=re.compile(
                r"performer|artist|act|band|musician", re.I)):
            pname_el = perf_el.find(["h2", "h3", "h4", "strong", "a"])
            pname    = clean(pname_el.get_text()) if pname_el else ""
            if not is_valid_performer(pname):
                continue
            info = extract_artist_info_from_block(perf_el, page_url)
            raw_perfs.append({"name": pname, **info})

        shows.append(make_show(
            name, extract_time(tel.get_text() if tel else text),
            "", adv, door, turl, raw_perfs
        ))
    return shows


def parse_generic(soup: BeautifulSoup, page_url: str = "") -> list[dict]:
    shows = []
    seen  = set()
    PAT   = re.compile(
        r'\b(event|show|gig|performance|listing|concert|calendar[-_]?item|program)\b', re.I
    )
    for el in soup.find_all(True, class_=PAT):
        text = el.get_text(" ", strip=True)
        if not has_today(text) or len(text) < 8:
            continue
        heading = (
            el.find(["h1", "h2", "h3", "h4"])
            or el.find(class_=re.compile(r"title|name|heading", re.I))
            or el.find("strong")
        )
        name = clean(heading.get_text()) if heading else clean(text[:80])
        if not name or name in seen or len(name) < 3:
            continue
        seen.add(name)
        adv, door = extract_prices(text)
        turl = ""
        tl = el.find("a", href=re.compile(r"ticket|buy|register|eventbrite|dice\.fm", re.I))
        if tl:
            turl = tl.get("href", "")

        # Look for performer blocks nested inside the event element
        raw_perfs = []
        for perf_el in el.find_all(class_=re.compile(
                r"performer|artist|act|band|musician|support|opener", re.I)):
            pname_el = perf_el.find(["h2", "h3", "h4", "strong", "a"])
            pname    = clean(pname_el.get_text()) if pname_el else ""
            if not is_valid_performer(pname):
                continue
            info = extract_artist_info_from_block(perf_el, page_url)
            raw_perfs.append({"name": pname, **info})

        # Fallback: performer names linked to social/artist pages
        if not raw_perfs:
            for a in el.find_all("a", href=True):
                href  = a.get("href", "")
                pname = clean(a.get_text())
                kind  = classify_url(href)
                if kind in SOCIAL_FIELDS and is_valid_performer(pname):
                    info = {kind: href}
                    raw_perfs.append({"name": pname, **info})

        shows.append(make_show(name, extract_time(text), "", adv, door, turl, raw_perfs))
    return shows


def parse_text_scan(soup: BeautifulSoup, page_url: str = "") -> list[dict]:
    shows = []
    seen  = set()
    body  = soup.find("body")
    if not body:
        return shows
    for el in body.find_all(["li", "tr", "div", "article", "section", "p"]):
        text = el.get_text(" ", strip=True)
        if not has_today(text) or len(text) > 300 or len(text) < 8:
            continue
        heading = (
            el.find(["h1", "h2", "h3", "h4", "strong", "b"])
            or el.find("a")
        )
        name = clean(heading.get_text()) if heading else clean(text[:80])
        if not name or name in seen or len(name) < 3:
            continue
        if not is_valid_performer(name):
            continue
        seen.add(name)
        adv, door = extract_prices(text)

        # Even in text_scan, try to pull any artist info from the block
        info = extract_artist_info_from_block(el, page_url)
        raw_perfs = [{"name": name, **info}] if any(info.values()) else []

        shows.append(make_show(name, extract_time(text), "", adv, door, raw_perfs=raw_perfs))
    return shows


# ══════════════════════════════════════════════════════════════════════════
# PAGE FETCH + VENUE SCRAPE
# ══════════════════════════════════════════════════════════════════════════

async def fetch_page(page, url: str) -> BeautifulSoup | None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_MS)
        await page.wait_for_timeout(SETTLE_MS)
        return BeautifulSoup(await page.content(), "lxml")
    except PWTimeout:
        log.warning("  timeout: %s", url)
        return None
    except Exception as exc:
        log.warning("  fetch error: %s", exc)
        return None


async def scrape_venue(venue: dict, page) -> list[dict]:
    url = venue.get("calendar_url") or venue.get("website")
    if not url:
        return []
    soup = await fetch_page(page, url)
    if not soup:
        return []
    for parser in [parse_jsonld, parse_tribe, parse_generic, parse_text_scan]:
        shows = parser(soup, page_url=url)
        if shows:
            log.info("  %-22s  %d show(s)", parser.__name__, len(shows))
            return shows
    return []


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

async def main():
    log.info("=== NYC Music Map Scraper  date=%s ===", TODAY_ISO)

    init_db()
    venues = json.loads(VENUES_FILE.read_text())
    log.info("Loaded %d venues", len(venues))

    results: dict[str, list] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            headless=True,
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        await ctx.route(
            re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|woff2?|ttf|otf|mp4|mp3|ico)$", re.I),
            lambda r: r.abort()
        )

        sem = asyncio.Semaphore(CONCURRENCY)

        async def process(idx: int, venue: dict):
            async with sem:
                vid  = str(venue["id"])
                name = venue["name"]
                log.info("[%d/%d] %s", idx, len(venues), name)
                page = await ctx.new_page()
                try:
                    shows = await scrape_venue(venue, page)
                    if shows:
                        results[vid] = shows
                except Exception as exc:
                    log.warning("  error: %s", exc)
                finally:
                    await page.close()
                await asyncio.sleep(REQ_DELAY)

        await asyncio.gather(*[process(i + 1, v) for i, v in enumerate(venues)])
        await browser.close()

    output = {
        "generated_at": datetime.now(NYC_TZ).isoformat(),
        "date":         TODAY_ISO,
        "events":       results,
    }
    EVENTS_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    venue_count = len(results)
    show_count  = sum(len(s) for s in results.values())
    log.info("=== Done — %d shows at %d venues ===", show_count, venue_count)


if __name__ == "__main__":
    asyncio.run(main())

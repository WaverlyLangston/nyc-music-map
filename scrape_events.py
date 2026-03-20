#!/usr/bin/env python3
"""
scrape_events.py
----------------
Daily scraper for NYC Music Map. Uses Playwright (headless Chromium) so
JavaScript-heavy pages render fully. Five parsing strategies in reliability
order. Performer names resolved through artist_db with URL fingerprinting.

Output: events.json (only venues with shows today appear)
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# Allow importing artist_db from same directory
sys.path.insert(0, str(Path(__file__).parent))
import artist_db as db

BASE_DIR    = Path(__file__).resolve().parent.parent
VENUES_FILE = BASE_DIR / "venues.json"
EVENTS_FILE = BASE_DIR / "events.json"

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

LASTFM_KEY  = os.environ.get("LASTFM_API_KEY", "")
CONCURRENCY = 4
PAGE_MS     = 22000
SETTLE_MS   = 2500
REQ_DELAY   = 1.2

MB_RATE     = 1.1   # seconds between MusicBrainz calls
_last_mb    = 0.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Utilities ──────────────────────────────────────────────────────────────

def clean(s):
    return " ".join(str(s).split()).strip()


def has_today(text):
    t = text.lower()
    return any(n in t for n in DATE_NEEDLES)


def extract_time(text):
    m = re.search(r'\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b', text, re.I)
    return m.group(1).strip().upper() if m else ""


def extract_prices(text):
    t = text.lower()
    if re.search(r'\bfree\b', t):
        return "Free", "Free"
    ad = re.search(
        r'\$(\d+(?:\.\d{2})?)\s*(?:adv(?:ance)?)\s*[/,|]\s*\$(\d+(?:\.\d{2})?)\s*(?:door|dos)',
        text, re.I)
    if ad:
        return f"${ad.group(1)}", f"${ad.group(2)}"
    door = re.search(r'\$(\d+(?:\.\d{2})?)\s*(?:at\s*the\s*)?door', text, re.I)
    if door:
        return "", f"${door.group(1)}"
    adv = re.search(r'\$(\d+(?:\.\d{2})?)\s*(?:adv(?:ance)?)', text, re.I)
    if adv:
        return f"${adv.group(1)}", ""
    rng = re.search(r'\$(\d+)\s*[-\u2013]\s*\$(\d+)', text)
    if rng:
        return f"${rng.group(1)}", f"${rng.group(2)}"
    single = re.search(r'\$(\d+(?:\.\d{2})?)', text)
    if single:
        p = f"${single.group(1)}"
        return p, p
    return "", ""


def make_show(name="", start_time="", end_time="",
              advance="", door="", ticket_url="", performers=None):
    return {
        "name":                 clean(name),
        "start_time":           start_time,
        "end_time":             end_time,
        "ticket_price_advance": advance,
        "ticket_price_door":    door,
        "ticket_url":           ticket_url,
        "performers":           performers or [],
    }


# ── Parsing strategies ─────────────────────────────────────────────────────

def parse_jsonld(soup):
    shows = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data  = json.loads(tag.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict): continue
                if item.get("@type") not in ("Event","MusicEvent","Concert"): continue
                start = item.get("startDate","")
                if not start or TODAY_ISO not in start: continue
                name = clean(item.get("name","Event"))
                try:
                    dt = datetime.fromisoformat(start)
                    st = dt.strftime("%-I:%M %p")
                except Exception:
                    st = extract_time(start)
                end_t = ""
                if item.get("endDate"):
                    try: end_t = datetime.fromisoformat(item["endDate"]).strftime("%-I:%M %p")
                    except Exception: pass
                offers = item.get("offers",{})
                if isinstance(offers,list): offers = offers[0] if offers else {}
                adv=door=tick=""
                if isinstance(offers,dict):
                    low=str(offers.get("lowPrice",""))
                    high=str(offers.get("highPrice",""))
                    avail=str(offers.get("availability",""))
                    tick=offers.get("url","")
                    if low=="0" or "free" in avail.lower(): adv=door="Free"
                    elif low:
                        p = f"${low}"+(f"-${high}" if high and high!=low else "")
                        adv=door=p
                raw_perfs=[]
                for p in (item.get("performer") or []):
                    pname=p.get("name","") if isinstance(p,dict) else str(p)
                    purl =p.get("url","")  if isinstance(p,dict) else ""
                    if pname:
                        raw_perfs.append({"name":clean(pname),"urls":[purl] if purl else []})
                shows.append(make_show(name,st,end_t,adv,door,tick,raw_perfs))
        except Exception:
            pass
    return shows


def parse_tribe(soup):
    shows = []
    for art in soup.find_all("article", class_=re.compile(r"tribe",re.I)):
        text = art.get_text(" ",strip=True)
        if not has_today(text): continue
        title = (art.find(class_=re.compile(r"tribe-event-name|entry-title",re.I))
                 or art.find(["h1","h2","h3","h4"]))
        if not title: continue
        name = clean(title.get_text())
        tel  = art.find(class_=re.compile(r"tribe-event-time|tribe-events-start-time",re.I))
        pel  = art.find(class_=re.compile(r"tribe-ticket|tribe-cost",re.I))
        turl = ""
        tl   = art.find("a",href=re.compile(r"ticket|register|buy",re.I))
        if tl: turl = tl.get("href","")
        adv,door = extract_prices(pel.get_text() if pel else text)
        raw_perfs = []
        for a in art.find_all("a",href=True):
            href=a.get("href",""); pname=clean(a.get_text())
            if pname and len(pname)>2 and "ticket" not in href.lower():
                raw_perfs.append({"name":pname,"urls":[href]})
        shows.append(make_show(name,extract_time(tel.get_text() if tel else text),
                                "",adv,door,turl,raw_perfs))
    return shows


def parse_generic(soup):
    shows=[];seen=set()
    PAT=re.compile(r'\b(event|show|gig|performance|listing|concert|calendar[-_]?item|program)\b',re.I)
    for el in soup.find_all(True,class_=PAT):
        text=el.get_text(" ",strip=True)
        if not has_today(text) or len(text)<8: continue
        heading=(el.find(["h1","h2","h3","h4"])
                 or el.find(class_=re.compile(r"title|name|heading",re.I))
                 or el.find("strong"))
        name=clean(heading.get_text()) if heading else clean(text[:80])
        if not name or name in seen or len(name)<3: continue
        seen.add(name)
        adv,door=extract_prices(text)
        tick=""
        tl=el.find("a",href=re.compile(r"ticket|buy|register|eventbrite|dice\.fm",re.I))
        if tl: tick=tl.get("href","")
        raw_perfs=[]
        for a in el.find_all("a",href=True):
            href=a.get("href",""); pname=clean(a.get_text())
            if pname and len(pname)>2:
                raw_perfs.append({"name":pname,"urls":[href]})
        shows.append(make_show(name,extract_time(text),"",adv,door,tick,raw_perfs))
    return shows


def parse_text_scan(soup):
    shows=[];seen=set()
    body=soup.find("body")
    if not body: return shows
    for el in body.find_all(["li","tr","div","article","section","p"]):
        text=el.get_text(" ",strip=True)
        if not has_today(text) or len(text)>500 or len(text)<8: continue
        heading=el.find(["h1","h2","h3","h4","strong","b"]) or el.find("a")
        name=clean(heading.get_text()) if heading else clean(text[:80])
        if not name or name in seen or len(name)<3: continue
        seen.add(name)
        adv,door=extract_prices(text)
        shows.append(make_show(name,extract_time(text),"",adv,door))
    return shows


# ── MusicBrainz lookup ─────────────────────────────────────────────────────

async def _mb_get(client, path, params):
    global _last_mb
    wait = MB_RATE - (time.monotonic() - _last_mb)
    if wait > 0:
        await asyncio.sleep(wait)
    r = await client.get(
        f"https://musicbrainz.org/ws/2/{path}",
        params={**params, "fmt":"json"},
        headers={"User-Agent":"NYCMusicMap/1.0 (github.com/your-repo)"},
        timeout=12,
    )
    _last_mb = time.monotonic()
    r.raise_for_status()
    return r.json()


def _score_mb_candidate(cand, search_name, context):
    score = int(fuzzy_score(search_name, cand.get("name","")) * 0.6)
    if search_name.lower() == cand.get("name","").lower(): score += 10
    disambig = cand.get("disambiguation","").lower()
    if disambig:
        for bad in ["tribute","cover","1920","1930","1940","1950"]:
            if bad in disambig: score -= 20; break
        if any(x in disambig for x in ["new york","brooklyn","nyc"]): score += 10
    area = (cand.get("area") or {}).get("name","").lower()
    if any(x in area for x in ["new york","brooklyn","manhattan"]): score += 8
    elif "united states" in area: score += 3
    tags = [t.get("name","").lower() for t in cand.get("tags",[])[:8]]
    venue_genre = context.get("genre","").lower()
    if venue_genre and tags:
        gw = set(re.split(r'\W+', venue_genre))
        tw = set(" ".join(tags).split())
        score += min(len(gw & tw) * 5, 15)
    life = cand.get("life-span",{})
    end  = life.get("end","")
    if end and int(end[:4]) < 2000: score -= 15
    return max(0, min(score, 100))


def fuzzy_score(a, b):
    from difflib import SequenceMatcher
    return int(SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100)


async def enrich_performer(client, raw_perf, context):
    """
    Resolve a raw_perf dict {"name": str, "urls": [str]} to a full
    artist data dict using artist_db + MusicBrainz + Last.fm.
    """
    name       = raw_perf.get("name","")
    found_urls = raw_perf.get("urls",[])

    if not name:
        return None

    # Skip known non-artist strings
    if db.is_nonartist(name):
        return None

    # Try local DB first
    res = db.resolve_artist(name, found_urls, context)

    if res["status"] in ("found","url_match"):
        mbid = res["mbid"]
        # Store any new URL fingerprints we found
        for url in found_urls:
            db.store_url_fingerprint(url, mbid)
        return db.get_artist_data(mbid, name)

    if res["status"] in ("ignored","nonartist"):
        return None

    # Not in DB yet — query MusicBrainz
    try:
        data = await _mb_get(client, "artist/", {"query":f'artist:"{name}"',"limit":"5"})
        candidates = data.get("artists",[])
    except Exception as exc:
        log.debug("MB search failed for '%s': %s", name, exc)
        candidates = []

    if candidates:
        scored = sorted(
            [{**c, "_score": _score_mb_candidate(c, name, context)} for c in candidates],
            key=lambda x: x["_score"], reverse=True
        )
        best = scored[0]
        score = best["_score"]

        if score >= db.AUTO_ACCEPT:
            mbid = best["id"]
            tags = [t.get("name","") for t in best.get("tags",[])[:10]]
            db.upsert_artist(
                mbid, best.get("name",name),
                best.get("disambiguation",""),
                (best.get("area") or {}).get("name",""),
                tags, score,
            )
            db.store_alias(name, mbid, score)

            # Fetch URL relations
            try:
                detail = await _mb_get(client, f"artist/{mbid}", {"inc":"url-rels"})
                for rel in detail.get("relations",[]):
                    url = rel.get("url",{}).get("resource","")
                    if url:
                        db.store_url_fingerprint(url, mbid)
                        kind = db.classify_url(url)
                        if kind in db.SOCIAL_FIELDS:
                            db.upsert_source(mbid, "musicbrainz", kind, url)
                        elif kind == "website":
                            db.upsert_source(mbid, "musicbrainz", "website", url)
            except Exception:
                pass

            # Store any scraped URLs as fingerprints too
            for url in found_urls:
                db.store_url_fingerprint(url, mbid)
                kind = db.classify_url(url)
                if kind in db.SOCIAL_FIELDS:
                    db.upsert_source(mbid, "own_website", kind, url)
                elif kind == "website":
                    db.upsert_source(mbid, "own_website", "website", url)

            # Last.fm bio
            if LASTFM_KEY:
                try:
                    r = await client.get(
                        "http://ws.audioscrobbler.com/2.0/",
                        params={"method":"artist.getinfo","artist":name,
                                "api_key":LASTFM_KEY,"format":"json","autocorrect":"1"},
                        timeout=8,
                    )
                    bio = r.json().get("artist",{}).get("bio",{}).get("summary","")
                    bio = re.sub(r'<a href="https://www\.last\.fm[^"]*"[^>]*>.*?</a>\.?','',bio)
                    bio = re.sub(r'<[^>]+>','',bio).strip()
                    bio = clean(bio)
                    if bio:
                        db.upsert_source(mbid, "lastfm", "description", bio[:500])
                except Exception:
                    pass

            return db.get_artist_data(mbid, name)

        elif score >= db.NEEDS_REVIEW:
            db._store_unresolved(name, db.normalize_name(name),
                                 scored[:5], context, "pending")
        # else: score too low, skip entirely

    # Return name-only if we couldn't resolve
    return db.get_artist_data(None, name)


# ── Page fetcher ───────────────────────────────────────────────────────────

async def fetch_page(page, url):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_MS)
        await page.wait_for_timeout(SETTLE_MS)
        html = await page.content()
        return BeautifulSoup(html, "lxml")
    except PWTimeout:
        log.warning("  timeout: %s", url)
        return None
    except Exception as exc:
        log.warning("  fetch error: %s", exc)
        return None


# ── Scrape one venue ───────────────────────────────────────────────────────

async def scrape_venue(venue, page, client):
    url = venue.get("calendar_url") or venue.get("website")
    if not url:
        return []

    soup = await fetch_page(page, url)
    if not soup:
        return []

    raw_shows = []
    for parser in [parse_jsonld, parse_tribe, parse_generic, parse_text_scan]:
        raw_shows = parser(soup)
        if raw_shows:
            log.info("  parser: %s  shows: %d", parser.__name__, len(raw_shows))
            break

    if not raw_shows:
        return []

    # Build context for MusicBrainz scoring
    context = {
        "venue":  venue.get("name",""),
        "borough": venue.get("borough",""),
        "genre":  venue.get("genre",""),
    }

    # Enrich each show's performers
    final_shows = []
    for show in raw_shows:
        raw_perfs   = show.get("performers") or []
        enriched    = []
        for rp in raw_perfs:
            # raw_perf is either a dict {"name":..,"urls":[..]} or just a string
            if isinstance(rp, str):
                rp = {"name": rp, "urls": []}
            result = await enrich_performer(client, rp, context)
            if result:
                enriched.append(result)

        final_shows.append({
            "name":                 show["name"],
            "start_time":           show["start_time"],
            "end_time":             show["end_time"],
            "ticket_price_advance": show["ticket_price_advance"],
            "ticket_price_door":    show["ticket_price_door"],
            "ticket_url":           show["ticket_url"],
            "performers":           enriched,
        })

    return final_shows


# ── Main ───────────────────────────────────────────────────────────────────

async def main():
    log.info("=== NYC Music Map Scraper  date=%s ===", TODAY_ISO)
    db.init_db()

    venues = json.loads(VENUES_FILE.read_text())
    log.info("Loaded %d venues", len(venues))

    results = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"],
            headless=True,
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width":1280,"height":900},
        )
        # Block media to speed up loading
        await ctx.route(
            re.compile(r"\.(png|jpg|jpeg|gif|webp|svg|woff2?|ttf|otf|mp4|mp3)$",re.I),
            lambda r: r.abort()
        )

        async with httpx.AsyncClient(follow_redirects=True) as client:
            sem = asyncio.Semaphore(CONCURRENCY)

            async def process(venue):
                async with sem:
                    vid  = str(venue["id"])
                    name = venue["name"]
                    log.info("[%d/%d] %s", venues.index(venue)+1, len(venues), name)
                    page = await ctx.new_page()
                    try:
                        shows = await scrape_venue(venue, page, client)
                        if shows:
                            results[vid] = shows
                    except Exception as exc:
                        log.warning("  error scraping %s: %s", name, exc)
                    finally:
                        await page.close()
                    await asyncio.sleep(REQ_DELAY)

            await asyncio.gather(*[process(v) for v in venues])

        await browser.close()

    # Write events.json
    output = {
        "generated_at": datetime.now(NYC_TZ).isoformat(),
        "date":         TODAY_ISO,
        "events":       results,
    }
    EVENTS_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    # Export review queue
    n_review = db.export_review_queue()

    venue_count = len(results)
    show_count  = sum(len(s) for s in results.values())
    log.info("=== Done — %d shows at %d venues ===", show_count, venue_count)
    if n_review:
        log.info("=== %d artists need review — see review_queue.json ===", n_review)


if __name__ == "__main__":
    asyncio.run(main())

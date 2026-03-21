#!/usr/bin/env python3
"""
enrich_artists.py
-----------------
Fills in artist data that wasn't found on venue pages.

Checks artists.db for any fields still empty, then queries:
  1. MusicBrainz  — MBID, social links, website (free, no key)
  2. Last.fm      — bio/description (free key from last.fm/api)

Only fills gaps — never overwrites data already sourced from
a venue page or a manual override.

Usage:
    python scripts/enrich_artists.py              # all unenriched
    python scripts/enrich_artists.py --limit 50   # batch of 50
    python scripts/enrich_artists.py --name "Mdou Moctar"
    python scripts/enrich_artists.py --stats
    python scripts/enrich_artists.py --re-enrich  # refresh all

Environment:
    LASTFM_API_KEY   — from last.fm/api/account/create (optional)
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse

import httpx

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_FILE    = BASE_DIR / "artists.db"
LASTFM_KEY = os.environ.get("LASTFM_API_KEY", "")

MB_BASE    = "https://musicbrainz.org/ws/2"
MB_HEADERS = {"User-Agent": "NYCMusicMap/1.0 (github.com/your-repo)"}
MB_RATE    = 1.1

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
_last_mb = 0.0


# ══════════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════════

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def needs_enrichment(row: dict) -> dict:
    """
    Return a dict of which fields are still missing for this artist.
    We never touch fields that already have data from 'venue' or 'manual' source.
    """
    missing = {}
    if not row.get("mbid"):
        missing["mbid"] = True
    if not row.get("description"):
        missing["description"] = True
    elif row.get("description_src") not in ("venue", "manual"):
        missing["description"] = True   # can still improve
    if not row.get("website") or row.get("website_src") not in ("venue", "manual"):
        missing["website"] = True
    for f in SOCIAL_FIELDS:
        if not row.get(f) or row.get(f"{f}_src") not in ("venue", "manual"):
            missing[f] = True
    return missing


def get_unenriched(limit: int | None = None) -> list[dict]:
    conn = get_db()
    q    = ("SELECT * FROM artists WHERE enriched = 0 AND locked = 0 "
            "ORDER BY seen_count DESC")
    if limit:
        q += f" LIMIT {limit}"
    rows = [dict(r) for r in conn.execute(q).fetchall()]
    conn.close()
    return rows


def get_all_for_reenrich(limit: int | None = None) -> list[dict]:
    conn = get_db()
    q    = "SELECT * FROM artists WHERE locked = 0 ORDER BY seen_count DESC"
    if limit:
        q += f" LIMIT {limit}"
    rows = [dict(r) for r in conn.execute(q).fetchall()]
    conn.close()
    return rows


def get_one(name: str) -> dict | None:
    conn = get_db()
    row  = conn.execute(
        "SELECT * FROM artists WHERE name_lower = ?", (name.lower().strip(),)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def save_enriched(name_lower: str, data: dict, missing: dict):
    """
    Write enrichment results. Only writes to fields that were
    identified as missing/improvable AND not locked/overridden.
    """
    conn = get_db()
    now  = datetime.utcnow().isoformat()

    # Fetch current overrides so we never step on them
    overrides = {
        r["field"] for r in conn.execute(
            "SELECT field FROM artist_overrides WHERE name_lower = ?",
            (name_lower,)
        ).fetchall()
    }

    updates = ["enriched = 1", "enriched_at = ?"]
    params  = [now]

    def write(field: str, value: str, src: str):
        if not value:
            return
        if field in overrides:
            return
        # Only write if this field was flagged as missing
        if field not in missing:
            return
        updates.append(f"{field} = ?")
        params.append(value)
        if f"{field}_src" in (  # only fields that have a _src column
            ["description_src", "website_src"]
            + [f"{f}_src" for f in SOCIAL_FIELDS]
        ):
            updates.append(f"{field}_src = ?")
            params.append(src)

    write("mbid",           data.get("mbid", ""),           "musicbrainz")
    write("disambiguation", data.get("disambiguation", ""), "musicbrainz")
    write("description",    data.get("description", ""),    data.get("description_src", "lastfm"))
    write("website",        data.get("website", ""),        "musicbrainz")
    for f in SOCIAL_FIELDS:
        write(f, data.get(f, ""), "musicbrainz")

    params.append(name_lower)
    conn.execute(
        f"UPDATE artists SET {', '.join(updates)} WHERE name_lower = ?",
        params
    )

    # Store URL fingerprints
    for f in ["website"] + SOCIAL_FIELDS:
        url = data.get(f, "")
        if url:
            norm = normalize_url(url)
            if norm:
                conn.execute(
                    "INSERT OR IGNORE INTO artist_urls "
                    "(url_normalized, name_lower, url_type) VALUES (?,?,?)",
                    (norm, name_lower, f)
                )

    conn.commit()
    conn.close()


def print_stats():
    conn  = get_db()
    total = conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
    enr   = conn.execute("SELECT COUNT(*) FROM artists WHERE enriched=1").fetchone()[0]
    venp  = conn.execute(
        "SELECT COUNT(*) FROM artists WHERE description_src='venue' "
        "OR instagram_src='venue' OR bandcamp_src='venue'"
    ).fetchone()[0]
    locked = conn.execute("SELECT COUNT(*) FROM artists WHERE locked=1").fetchone()[0]
    ovr    = conn.execute("SELECT COUNT(*) FROM artist_overrides").fetchone()[0]
    conn.close()
    print(f"\n  Artists in database : {total}")
    print(f"  Enriched (MB/Last.fm): {enr}")
    print(f"  Have venue-scraped data: {venp}")
    print(f"  Locked records      : {locked}")
    print(f"  Manual overrides    : {ovr}\n")


# ══════════════════════════════════════════════════════════════════════════
# URL UTILITIES
# ══════════════════════════════════════════════════════════════════════════

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


def extract_urls(relations: list[dict]) -> dict:
    result = {"website": ""}
    for f in SOCIAL_FIELDS:
        result[f] = ""
    for rel in relations:
        url  = rel.get("url", {}).get("resource", "")
        if not url:
            continue
        kind = classify_url(url)
        if kind in result and not result[kind]:
            result[kind] = url
        elif kind == "website" and not result["website"]:
            result["website"] = url
    return result


# ══════════════════════════════════════════════════════════════════════════
# MUSICBRAINZ
# ══════════════════════════════════════════════════════════════════════════

async def mb_get(client: httpx.AsyncClient, path: str, params: dict) -> dict:
    global _last_mb
    wait = MB_RATE - (time.monotonic() - _last_mb)
    if wait > 0:
        await asyncio.sleep(wait)
    r = await client.get(
        f"{MB_BASE}/{path}",
        params={**params, "fmt": "json"},
        headers=MB_HEADERS,
        timeout=12,
    )
    _last_mb = time.monotonic()
    r.raise_for_status()
    return r.json()


def score_candidate(cand: dict, name: str) -> int:
    score = int(SequenceMatcher(None, name.lower(),
                                cand.get("name", "").lower()).ratio() * 60)
    if name.lower() == cand.get("name", "").lower():
        score += 10
    disambig = cand.get("disambiguation", "").lower()
    if disambig:
        for bad in ["tribute", "cover", "1920", "1930", "1940", "1950"]:
            if bad in disambig:
                score -= 20
                break
        if any(x in disambig for x in ["new york", "brooklyn", "nyc"]):
            score += 10
    area = (cand.get("area") or {}).get("name", "").lower()
    if any(x in area for x in ["new york", "brooklyn", "manhattan"]):
        score += 8
    elif "united states" in area:
        score += 3
    life = cand.get("life-span", {})
    if life.get("end") and int(life["end"][:4]) < 2000:
        score -= 15
    return max(0, min(score, 100))


async def lookup_musicbrainz(
    client: httpx.AsyncClient,
    name: str,
    known_urls: list[str]
) -> dict:
    """
    Search MusicBrainz for an artist.
    Uses known_urls (from venue scraping) to help disambiguate.
    Returns enrichment dict or {}.
    """
    try:
        data       = await mb_get(client, "artist/", {"query": f'artist:"{name}"', "limit": "5"})
        candidates = data.get("artists", [])
        if not candidates:
            return {}

        scored = sorted(
            [{**c, "_score": score_candidate(c, name)} for c in candidates],
            key=lambda x: x["_score"], reverse=True
        )

        # If multiple candidates score similarly, use URL fingerprints to pick
        top_score = scored[0]["_score"]
        finalists = [c for c in scored if c["_score"] >= top_score - 10]

        chosen = None
        if len(finalists) > 1 and known_urls:
            # Fetch URL relations for each finalist and compare against known_urls
            known_norms = {normalize_url(u) for u in known_urls if u}
            for cand in finalists:
                try:
                    detail = await mb_get(
                        client, f"artist/{cand['id']}", {"inc": "url-rels"}
                    )
                    cand_urls = {
                        normalize_url(r.get("url", {}).get("resource", ""))
                        for r in detail.get("relations", [])
                    }
                    if known_norms & cand_urls:
                        chosen = (cand, detail)
                        log.info("  URL fingerprint resolved to '%s'", cand.get("name"))
                        break
                except Exception:
                    pass

        if not chosen:
            best = finalists[0]
            if best["_score"] < 55:
                log.debug("  MB: low confidence (%d) for '%s'", best["_score"], name)
                return {}
            try:
                detail = await mb_get(client, f"artist/{best['id']}", {"inc": "url-rels"})
            except Exception:
                detail = best
            chosen = (best, detail)

        cand, detail = chosen
        log.info("  MB: '%s' [score=%d]", cand.get("name"), cand["_score"])

        urls    = extract_urls(detail.get("relations", []))
        return {
            "mbid":           cand.get("id", ""),
            "disambiguation": cand.get("disambiguation", ""),
            **urls,
        }

    except Exception as exc:
        log.debug("  MB error for '%s': %s", name, exc)
        return {}


# ══════════════════════════════════════════════════════════════════════════
# LAST.FM
# ══════════════════════════════════════════════════════════════════════════

async def lookup_lastfm(client: httpx.AsyncClient, name: str) -> str:
    if not LASTFM_KEY:
        return ""
    try:
        r = await client.get(
            "http://ws.audioscrobbler.com/2.0/",
            params={
                "method": "artist.getinfo", "artist": name,
                "api_key": LASTFM_KEY, "format": "json", "autocorrect": "1",
            },
            timeout=10,
        )
        r.raise_for_status()
        bio = r.json().get("artist", {}).get("bio", {}).get("summary", "")
        bio = re.sub(r'<a href="https://www\.last\.fm[^"]*"[^>]*>.*?</a>\.?', '', bio)
        bio = re.sub(r'<[^>]+>', '', bio).strip()
        bio = " ".join(bio.split())
        return bio[:500] if bio else ""
    except Exception as exc:
        log.debug("  Last.fm error for '%s': %s", name, exc)
        return ""


# ══════════════════════════════════════════════════════════════════════════
# ENRICHMENT PIPELINE
# ══════════════════════════════════════════════════════════════════════════

async def enrich_one(client: httpx.AsyncClient, artist: dict):
    name       = artist["name_display"]
    name_lower = artist["name_lower"]
    missing    = needs_enrichment(artist)

    if not missing:
        log.info("  '%s' — nothing missing, skipping", name)
        save_enriched(name_lower, {}, {})   # mark as enriched
        return

    log.info("Enriching: %s  (missing: %s)", name, ", ".join(missing.keys()))

    # Collect known URLs from the artist_urls table (from venue scraping)
    conn        = get_db()
    known_urls  = [
        r["url_normalized"]
        for r in conn.execute(
            "SELECT url_normalized FROM artist_urls WHERE name_lower = ?",
            (name_lower,)
        ).fetchall()
    ]
    conn.close()

    enriched = {}

    # MusicBrainz — only if we need MBID or any URL-based field
    needs_mb = (
        "mbid" in missing
        or "website" in missing
        or any(f in missing for f in SOCIAL_FIELDS)
    )
    if needs_mb:
        enriched.update(await lookup_musicbrainz(client, name, known_urls))

    # Last.fm bio — only if description is still missing after venue scraping
    if "description" in missing:
        bio = await lookup_lastfm(client, name)
        if bio:
            enriched["description"]     = bio
            enriched["description_src"] = "lastfm"
            log.info("  Last.fm bio: %d chars", len(bio))

    save_enriched(name_lower, enriched, missing)


async def run(artists: list[dict]):
    if not artists:
        log.info("Nothing to enrich.")
        return
    log.info("Enriching %d artist(s)...", len(artists))
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for i, artist in enumerate(artists, 1):
            log.info("[%d/%d]", i, len(artists))
            try:
                await enrich_one(client, artist)
            except Exception as exc:
                log.warning("  Failed '%s': %s", artist.get("name_display"), exc)
    log.info("=== Enrichment complete ===")


# ══════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="Fill missing artist data from MusicBrainz + Last.fm"
    )
    ap.add_argument("--limit",     type=int,  default=None)
    ap.add_argument("--name",      type=str,  default=None)
    ap.add_argument("--stats",     action="store_true")
    ap.add_argument("--re-enrich", action="store_true")
    args = ap.parse_args()

    if not DB_FILE.exists():
        log.error("artists.db not found. Run scrape_events.py first.")
        raise SystemExit(1)

    if args.stats:
        print_stats()
        return

    if args.name:
        artist = get_one(args.name)
        if not artist:
            log.error("Artist '%s' not in database.", args.name)
            raise SystemExit(1)
        asyncio.run(run([artist]))
        return

    artists = get_all_for_reenrich(args.limit) if args.re_enrich \
              else get_unenriched(args.limit)

    print_stats()
    asyncio.run(run(artists))
    print_stats()


if __name__ == "__main__":
    main()

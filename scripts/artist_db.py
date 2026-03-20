#!/usr/bin/env python3
"""
artist_db.py — SQLite artist database with URL-fingerprint disambiguation.

Resolution pipeline for a performer name + scraped URLs:
  1. name_aliases lookup  (instant for already-resolved names)
  2. If multiple MBIDs share the name → URL fingerprint match
  3. MusicBrainz scored search → auto-accept or queue for review
  4. Return best available data (or name-only if unresolved)
"""

import json
import logging
import re
import sqlite3
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse

log = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_FILE    = BASE_DIR / "artists.db"
QUEUE_FILE = BASE_DIR / "review_queue.json"

AUTO_ACCEPT  = 72
NEEDS_REVIEW = 42

SOURCE_PRIORITY = {
    "override": 0, "bandcamp": 1, "own_website": 2,
    "musicbrainz": 3, "lastfm": 4, "wikipedia": 5,
}

SOCIAL_FIELDS = [
    "instagram", "bandcamp", "facebook",
    "spotify", "twitter", "youtube", "soundcloud", "tiktok",
]

KNOWN_NONARTISTS = {
    "tba", "tbd", "t.b.a.", "t.b.d.", "to be announced",
    "special guest", "special guests", "surprise guest",
    "dj set", "dj tba", "live dj", "doors", "doors open",
    "various artists", "local support", "support tba",
    "open bar", "21+", "18+", "free", "sold out",
}


def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS artists (
            mbid           TEXT PRIMARY KEY,
            name           TEXT NOT NULL,
            name_lower     TEXT NOT NULL,
            disambiguation TEXT DEFAULT '',
            country        TEXT DEFAULT '',
            tags           TEXT DEFAULT '[]',
            confidence     INTEGER DEFAULT 0,
            created_at     TEXT NOT NULL,
            updated_at     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS artist_urls (
            url_normalized TEXT NOT NULL,
            mbid           TEXT NOT NULL REFERENCES artists(mbid),
            url_type       TEXT NOT NULL DEFAULT 'unknown',
            PRIMARY KEY (url_normalized, mbid)
        );

        CREATE TABLE IF NOT EXISTS artist_sources (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            mbid       TEXT NOT NULL REFERENCES artists(mbid),
            source     TEXT NOT NULL,
            field      TEXT NOT NULL,
            value      TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            UNIQUE(mbid, source, field)
        );

        CREATE TABLE IF NOT EXISTS artist_overrides (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            mbid       TEXT,
            name       TEXT NOT NULL,
            field      TEXT NOT NULL,
            value      TEXT NOT NULL,
            note       TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            UNIQUE(name, field)
        );

        CREATE TABLE IF NOT EXISTS name_aliases (
            alias      TEXT NOT NULL,
            mbid       TEXT NOT NULL REFERENCES artists(mbid),
            confidence INTEGER NOT NULL DEFAULT 0,
            source     TEXT NOT NULL DEFAULT 'auto',
            PRIMARY KEY (alias, mbid)
        );

        CREATE TABLE IF NOT EXISTS unresolved (
            name          TEXT PRIMARY KEY,
            name_lower    TEXT NOT NULL,
            venue_context TEXT DEFAULT '{}',
            candidates    TEXT DEFAULT '[]',
            seen_count    INTEGER DEFAULT 1,
            last_seen     TEXT NOT NULL,
            status        TEXT DEFAULT 'pending'
        );

        CREATE INDEX IF NOT EXISTS idx_artists_name  ON artists(name_lower);
        CREATE INDEX IF NOT EXISTS idx_aliases_alias ON name_aliases(alias);
        CREATE INDEX IF NOT EXISTS idx_urls_url      ON artist_urls(url_normalized);
        CREATE INDEX IF NOT EXISTS idx_sources_mbid  ON artist_sources(mbid);
    """)
    conn.commit()
    conn.close()
    log.info("Database ready: %s", DB_FILE)


def normalize_url(url):
    url = url.strip().lower()
    try:
        p    = urlparse(url if "://" in url else "https://" + url)
        host = p.netloc.replace("www.", "").strip(".")
        path = p.path.rstrip("/")
        return f"{host}{path}" if host else ""
    except Exception:
        return ""


def classify_url(url):
    u = url.lower()
    if "instagram.com"  in u: return "instagram"
    if ".bandcamp.com"  in u: return "bandcamp"
    if "facebook.com"   in u: return "facebook"
    if "open.spotify.com" in u or "spotify.com/artist" in u: return "spotify"
    if "twitter.com"    in u or "/x.com" in u: return "twitter"
    if "youtube.com"    in u or "youtu.be" in u: return "youtube"
    if "soundcloud.com" in u: return "soundcloud"
    if "tiktok.com"     in u: return "tiktok"
    return "website"


def extract_social_and_website(urls):
    social  = {f: "" for f in SOCIAL_FIELDS}
    website = ""
    for url in urls:
        if not url or url.startswith("#") or url.startswith("mailto"):
            continue
        kind = classify_url(url)
        if kind in social and not social[kind]:
            social[kind] = url
        elif kind == "website" and not website:
            website = url
    return social, website


def normalize_name(name):
    n = name.lower().strip()
    n = re.sub(r"['\u2018\u2019`]", "'", n)
    n = re.sub(r'\s+', ' ', n)
    return n


def is_nonartist(name):
    return normalize_name(name) in KNOWN_NONARTISTS


def fuzzy_score(a, b):
    return int(SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100)


def resolve_artist(name, found_urls=None, context=None):
    """
    Resolve a performer name to an MBID using the database and URL fingerprints.

    Returns dict:
      { mbid, status, confidence, artist }

    status values:
      "found"        - clean single match in name_aliases
      "url_match"    - disambiguated via URL fingerprint
      "ambiguous"    - multiple MBIDs, no URL match, sent to review
      "unresolved"   - seen before but still pending review
      "not_found"    - first time seeing this name
      "nonartist"    - known non-artist string (TBA, etc.)
      "ignored"      - manually marked to skip
    """
    if is_nonartist(name):
        return _r(None, "nonartist", 0)

    found_urls = [u for u in (found_urls or []) if u]
    context    = context or {}
    alias      = normalize_name(name)

    conn = get_db()
    rows = conn.execute(
        "SELECT mbid, confidence FROM name_aliases WHERE alias=?", (alias,)
    ).fetchall()
    conn.close()

    if not rows:
        conn = get_db()
        unres = conn.execute(
            "SELECT status FROM unresolved WHERE name_lower=?", (alias,)
        ).fetchone()
        conn.close()
        if unres:
            if unres["status"] == "ignored":
                return _r(None, "ignored", 0)
            _bump(alias)
            return _r(None, "unresolved", 0)
        return _r(None, "not_found", 0)

    if len(rows) == 1:
        mbid = rows[0]["mbid"]
        return _r(mbid, "found", rows[0]["confidence"], _get_artist(mbid))

    # Multiple MBIDs share this name — try URL fingerprinting
    if found_urls:
        candidate_mbids = {r["mbid"] for r in rows}
        conn = get_db()
        for url in found_urls:
            norm = normalize_url(url)
            if not norm:
                continue
            hit = conn.execute(
                "SELECT mbid FROM artist_urls WHERE url_normalized=?", (norm,)
            ).fetchone()
            if hit and hit["mbid"] in candidate_mbids:
                mbid = hit["mbid"]
                conn.close()
                log.info(
                    "  URL fingerprint resolved ambiguous '%s' -> %s via %s",
                    name, mbid, norm
                )
                return _r(mbid, "url_match", 95, _get_artist(mbid))
        conn.close()

    # Still ambiguous — queue for manual review
    candidates = [{"mbid": r["mbid"], "confidence": r["confidence"]} for r in rows]
    _store_unresolved(name, alias, candidates, context, "ambiguous_name")
    return _r(None, "ambiguous", 0)


def _r(mbid, status, confidence, artist=None):
    return {"mbid": mbid, "status": status,
            "confidence": confidence, "artist": artist}


def _get_artist(mbid):
    conn = get_db()
    row  = conn.execute("SELECT * FROM artists WHERE mbid=?", (mbid,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _bump(alias):
    conn = get_db()
    conn.execute(
        "UPDATE unresolved SET seen_count=seen_count+1, last_seen=? WHERE name_lower=?",
        (datetime.utcnow().isoformat(), alias)
    )
    conn.commit()
    conn.close()


def upsert_artist(mbid, name, disambiguation="", country="",
                  tags=None, confidence=0):
    conn = get_db()
    now  = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT INTO artists
               (mbid,name,name_lower,disambiguation,country,tags,confidence,created_at,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(mbid) DO UPDATE SET
               name=excluded.name, name_lower=excluded.name_lower,
               disambiguation=excluded.disambiguation, tags=excluded.tags,
               confidence=excluded.confidence, updated_at=excluded.updated_at""",
        (mbid, name, name.lower(), disambiguation, country,
         json.dumps(tags or []), confidence, now, now)
    )
    conn.commit()
    conn.close()


def store_alias(alias, mbid, confidence, source="auto"):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO name_aliases (alias,mbid,confidence,source) VALUES (?,?,?,?)",
        (normalize_name(alias), mbid, confidence, source)
    )
    conn.commit()
    conn.close()


def store_url_fingerprint(url, mbid):
    norm = normalize_url(url)
    if not norm:
        return
    conn = get_db()
    conn.execute(
        "INSERT OR IGNORE INTO artist_urls (url_normalized,mbid,url_type) VALUES (?,?,?)",
        (norm, mbid, classify_url(url))
    )
    conn.commit()
    conn.close()


def upsert_source(mbid, source, field, value):
    if not value or not str(value).strip():
        return
    conn = get_db()
    conn.execute(
        """INSERT INTO artist_sources (mbid,source,field,value,fetched_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(mbid,source,field) DO UPDATE SET
               value=excluded.value, fetched_at=excluded.fetched_at""",
        (mbid, source, field, str(value).strip(), datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()


def _store_unresolved(name, alias, candidates, context, status="pending"):
    conn = get_db()
    now  = datetime.utcnow().isoformat()
    ex   = conn.execute(
        "SELECT seen_count FROM unresolved WHERE name_lower=?", (alias,)
    ).fetchone()
    if ex:
        conn.execute(
            "UPDATE unresolved SET seen_count=seen_count+1,last_seen=?,"
            "candidates=?,venue_context=?,status=? WHERE name_lower=?",
            (now, json.dumps(candidates), json.dumps(context), status, alias)
        )
    else:
        conn.execute(
            "INSERT INTO unresolved "
            "(name,name_lower,venue_context,candidates,seen_count,last_seen,status) "
            "VALUES (?,?,?,?,1,?,?)",
            (name, alias, json.dumps(context), json.dumps(candidates), now, status)
        )
    conn.commit()
    conn.close()


def get_best_value(mbid, field, name=""):
    conn = get_db()
    ov   = conn.execute(
        "SELECT value FROM artist_overrides WHERE field=? AND (mbid=? OR name=?) "
        "ORDER BY CASE WHEN mbid IS NOT NULL THEN 0 ELSE 1 END LIMIT 1",
        (field, mbid or "", name)
    ).fetchone()
    if ov:
        conn.close()
        return ov["value"]
    if not mbid:
        conn.close()
        return ""
    rows = conn.execute(
        "SELECT source,value FROM artist_sources WHERE mbid=? AND field=?",
        (mbid, field)
    ).fetchall()
    conn.close()
    if not rows:
        return ""
    rows_sorted = sorted(rows, key=lambda r: SOURCE_PRIORITY.get(r["source"], 99))
    return rows_sorted[0]["value"]


def get_artist_data(mbid, name):
    return {
        "name":        name,
        "mbid":        mbid or "",
        "description": get_best_value(mbid or "", "description", name),
        "website":     get_best_value(mbid or "", "website", name),
        "social":      {f: get_best_value(mbid or "", f, name) for f in SOCIAL_FIELDS},
    }


def export_review_queue():
    conn  = get_db()
    rows  = conn.execute(
        "SELECT * FROM unresolved WHERE status IN ('pending','ambiguous_name') "
        "ORDER BY seen_count DESC"
    ).fetchall()
    conn.close()
    queue = []
    for row in rows:
        try:
            candidates = json.loads(row["candidates"] or "[]")
            context    = json.loads(row["venue_context"] or "{}")
        except Exception:
            candidates, context = [], {}
        queue.append({
            "name":       row["name"],
            "status":     row["status"],
            "seen_count": row["seen_count"],
            "last_seen":  row["last_seen"],
            "context":    context,
            "candidates": candidates,
            "fix":        'python scripts/manage_artists.py resolve "NAME" MBID',
        })
    QUEUE_FILE.write_text(json.dumps(queue, indent=2, ensure_ascii=False))
    return len(queue)


def add_override(name, field, value, mbid="", note=""):
    conn = get_db()
    conn.execute(
        """INSERT INTO artist_overrides (mbid,name,field,value,note,created_at)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(name,field) DO UPDATE SET value=excluded.value,note=excluded.note""",
        (mbid or None, name, field, value, note, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()


def resolve_manually(name, mbid):
    store_alias(name, mbid, confidence=100, source="manual")
    conn = get_db()
    conn.execute(
        "UPDATE unresolved SET status='resolved' WHERE name_lower=?",
        (normalize_name(name),)
    )
    conn.commit()
    conn.close()


def ignore_name(name):
    conn = get_db()
    conn.execute(
        "UPDATE unresolved SET status='ignored' WHERE name_lower=?",
        (normalize_name(name),)
    )
    conn.commit()
    conn.close()

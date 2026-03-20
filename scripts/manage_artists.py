#!/usr/bin/env python3
"""
manage_artists.py — CLI for reviewing and correcting artist data.

Run locally (not in GitHub Actions).

Commands:
  init                           Initialize the database
  queue                          Show artists needing review
  resolve "Name" MBID            Manually link a name to a MusicBrainz ID
  ignore  "Name"                 Mark as not a real artist (TBA, DJ Set, etc.)
  override "Name" field value    Manually set any field for an artist
  show "Name"                    Display everything we know about an artist
  urls "Name"                    Show all stored URLs for an artist

Examples:
  python scripts/manage_artists.py queue
  python scripts/manage_artists.py resolve "Ceremony" f27ec8db-af05-4f36-916e-3d57f91ecf7e
  python scripts/manage_artists.py ignore "TBA"
  python scripts/manage_artists.py override "Ceremony" description "NYC jazz trio, not the CA punk band"
  python scripts/manage_artists.py override "Ceremony" instagram "https://instagram.com/ceremonynyc"
  python scripts/manage_artists.py show "Ceremony"
"""

import json
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import artist_db as db

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")


def cmd_init():
    db.init_db()
    print(f"Database initialized at {db.DB_FILE}")


def cmd_queue():
    n = db.export_review_queue()
    if n == 0:
        print("Review queue is empty.")
        return
    print(f"{n} artist(s) need review. Full list: {db.QUEUE_FILE}\n")
    items = json.loads(db.QUEUE_FILE.read_text())
    for item in items[:10]:
        print(f"  '{item['name']}'  (seen {item['seen_count']}x, status: {item['status']})")
        for c in item["candidates"][:3]:
            mbid = c.get("mbid","")
            name = c.get("canonical_name", c.get("name",""))
            disamb = c.get("disambiguation","")
            score  = c.get("score", c.get("confidence",0))
            print(f"    [{score:3d}] {name}  |  {disamb}  |  {mbid}")
        print()
    if n > 10:
        print(f"  ... and {n-10} more in {db.QUEUE_FILE}")


def cmd_resolve(name, mbid):
    db.resolve_manually(name, mbid)
    print(f"Resolved '{name}' → {mbid}")


def cmd_ignore(name):
    db.ignore_name(name)
    print(f"'{name}' marked as ignored.")


def cmd_override(name, field, value):
    db.add_override(name, field, value)
    print(f"Override: '{name}'.{field} = '{value}'")


def cmd_show(name):
    res = db.resolve_artist(name)
    print(f"\nArtist: '{name}'")
    print(f"Status: {res['status']}  |  Confidence: {res['confidence']}")
    if res["mbid"]:
        data = db.get_artist_data(res["mbid"], name)
        print(f"MBID:   {res['mbid']}")
        desc = data["description"]
        print(f"Bio:    {desc[:150] + '...' if len(desc) > 150 else desc or '(none)'}")
        print(f"Web:    {data['website'] or '(none)'}")
        print("Social:")
        for k, v in data["social"].items():
            if v:
                print(f"  {k:12s} {v}")
    else:
        conn = db.get_db()
        unres = conn.execute(
            "SELECT * FROM unresolved WHERE name_lower=?",
            (db.normalize_name(name),)
        ).fetchone()
        conn.close()
        if unres:
            cands = json.loads(unres["candidates"] or "[]")
            print("Candidates in review queue:")
            for c in cands[:5]:
                mbid = c.get("mbid","")
                print(f"  mbid={mbid}  score={c.get('confidence',0)}")
            print(f"\nTo resolve:  python scripts/manage_artists.py resolve \"{name}\" <mbid>")


def cmd_urls(name):
    res = db.resolve_artist(name)
    if not res["mbid"]:
        print(f"'{name}' not resolved to an MBID yet.")
        return
    conn = db.get_db()
    rows = conn.execute(
        "SELECT url_normalized, url_type FROM artist_urls WHERE mbid=?",
        (res["mbid"],)
    ).fetchall()
    conn.close()
    print(f"Stored URL fingerprints for '{name}' (MBID {res['mbid']}):")
    for r in rows:
        print(f"  [{r['url_type']:12s}] {r['url_normalized']}")
    if not rows:
        print("  (none stored)")


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return
    cmd = args[0]
    if   cmd == "init"     and len(args) == 1:       cmd_init()
    elif cmd == "queue"    and len(args) == 1:       cmd_queue()
    elif cmd == "resolve"  and len(args) == 3:       cmd_resolve(args[1], args[2])
    elif cmd == "ignore"   and len(args) == 2:       cmd_ignore(args[1])
    elif cmd == "override" and len(args) == 4:       cmd_override(args[1], args[2], args[3])
    elif cmd == "show"     and len(args) == 2:       cmd_show(args[1])
    elif cmd == "urls"     and len(args) == 2:       cmd_urls(args[1])
    else:
        print("Unrecognized command. Run without arguments to see usage.")
        sys.exit(1)


if __name__ == "__main__":
    main()

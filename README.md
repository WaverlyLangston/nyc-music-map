# NYC Music Map

An interactive map of some music venues across all five NYC boroughs. Dots on the map mark each venue. Clicking a dot shows the day's shows — performers, start time, ticket price — scraped fresh every morning at 7am from the venues' own calendar pages.

---

## Repository structure

```
nyc-music-map/
├── index.html              ← the map website (served by GitHub Pages)
├── venues.json             ← 200 venues with addresses + coordinates
├── events.json             ← today's events, refreshed daily by GitHub Actions
├── requirements.txt        ← Python dependencies for the scraper
├── scripts/
│   ├── scrape_events.py    ← daily event scraper (run by GitHub Actions)
│   └── geocode_venues.py   ← one-time utility to verify/update coordinates
└── .github/
    └── workflows/
        └── daily_refresh.yml   ← GitHub Actions cron job
```


## Tech stack

| Layer | Technology |
|---|---|
| Map | Mapbox GL JS v3 |
| Venue data | Static `venues.json` in the repo |
| Event data | `events.json`, generated daily |
| Scraper | Python 3.12 + requests + BeautifulSoup |
| Automation | GitHub Actions (cron schedule) |
| Hosting | GitHub Pages (free, static) |
| Backend | None |

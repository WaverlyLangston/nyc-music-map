# NYC Music Map

An interactive map of 200 independent music venues across all five NYC boroughs. Dots on the map mark each venue. Clicking a dot shows the day's shows — performers, start time, ticket price — scraped fresh every morning at 7am from the venues' own calendar pages.

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

---

## Setup — step by step

### 1. Create the GitHub repository

1. Go to [github.com/new](https://github.com/new)
2. Name it `nyc-music-map` (or anything you like)
3. Set it to **Public**
4. Do **not** initialize with a README (you already have one)
5. Click **Create repository**

### 2. Push this code to GitHub

In your terminal:

```bash
cd nyc-music-map          # the folder containing this README

git init
git add .
git commit -m "initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/nyc-music-map.git
git push -u origin main
```

Replace `YOUR_USERNAME` with your GitHub username.

### 3. Add your Mapbox token

Open `index.html` and find this line near the bottom of the `<script>` block:

```js
const MAPBOX_TOKEN = "YOUR_MAPBOX_TOKEN_HERE";
```

Replace `YOUR_MAPBOX_TOKEN_HERE` with your actual Mapbox public token (starts with `pk.`).

You can find your token at [account.mapbox.com](https://account.mapbox.com).

> **Security note:** Mapbox public tokens (`pk.`) are designed to be exposed in front-end code. You can restrict your token to specific URLs in the Mapbox dashboard under *Access Tokens → Token Restrictions* to prevent abuse.

Commit and push:

```bash
git add index.html
git commit -m "add mapbox token"
git push
```

### 4. Enable GitHub Pages

1. Go to your repo on GitHub
2. Click **Settings** → **Pages** (in the left sidebar)
3. Under *Source*, select **Deploy from a branch**
4. Set branch to `main`, folder to `/ (root)`
5. Click **Save**

GitHub will give you a URL like:  
`https://YOUR_USERNAME.github.io/nyc-music-map/`

It may take 1–2 minutes to deploy the first time. After that, every push to `main` automatically redeploys.

### 5. Verify the map loads

Open your GitHub Pages URL in a browser. You should see:
- A minimal grey map of NYC
- ~200 dots across the five boroughs
- A prompt to allow location access (click Allow)
- The map will fly to your current location at zoom 14

Click any dot to see the venue card. Dark dots = venue in the dataset. Red dots = venue has events listed today.

---

## Daily event scraping — how it works

The file `.github/workflows/daily_refresh.yml` defines a GitHub Actions job that:

1. Runs automatically at **12:00 UTC (7am ET)** every day
2. Checks out the repository
3. Installs Python dependencies from `requirements.txt`
4. Runs `scripts/scrape_events.py`, which visits each venue's calendar page and extracts today's shows
5. Saves results to `events.json`
6. Commits and pushes `events.json` back to the repo
7. GitHub Pages automatically serves the updated file — no server needed

You can also trigger it manually:
1. Go to your repo → **Actions** tab
2. Click **Daily Event Scrape**
3. Click **Run workflow**

### What the scraper does

For each venue it:
- Fetches the venue's `calendar_url`
- Tries to extract today's events using multiple strategies:
  - **Schema.org JSON-LD** (the most reliable — works for Eventbrite, Dice, and sites that tag their events properly)
  - **WordPress "The Events Calendar"** markup
  - **Generic CSS class patterns** (Squarespace, custom sites)
  - **Text-scanning fallback** (looks for today's date anywhere on the page)
- Extracts: event name, start time, ticket price, performer list
- Saves all results to `events.json` keyed by venue ID

**Realistic expectations:** The scraper will successfully extract events from roughly 30–60% of venues on any given day. Venues that:
- Use Eventbrite or Dice (with JSON-LD): very reliable
- Use WordPress + The Events Calendar: reliable
- Use custom CMSes or heavy JavaScript rendering: will often return nothing

The map still shows all venues as dots regardless. The event card simply says "No events listed for today" when the scraper found nothing — which may mean no show, or may mean the page was unscrapeable.

### Improving scraper coverage over time

As you use the map, you can add venue-specific parsers to `scrape_events.py` for any venue that consistently fails. The scraper is intentionally modular for this reason — each venue can get its own parsing function if needed.

---

## Adding or editing venues

Edit `venues.json` directly. Each venue entry looks like:

```json
{
  "id": 1,
  "name": "Village Vanguard",
  "address": "178 7th Ave S, New York, NY 10014",
  "borough": "Manhattan",
  "neighborhood": "West Village",
  "lat": 40.7337,
  "lng": -74.0027,
  "website": "https://villagevanguard.com/",
  "calendar_url": "https://villagevanguard.com/tickets/"
}
```

Required fields: `id`, `name`, `address`, `borough`, `lat`, `lng`, `website`, `calendar_url`

After editing, commit and push. The map updates immediately.

### Re-geocoding venues

If you add venues without coordinates, run the geocoder:

```bash
export MAPBOX_TOKEN=pk.your_token_here
python scripts/geocode_venues.py
```

This will add `lat` and `lng` to any venue that's missing them using the Mapbox Geocoding API. Commit the updated `venues.json`.

---

## Running the scraper locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the scraper
python scripts/scrape_events.py
```

This writes `events.json` in the repo root. Check the log output to see which venues returned events and which failed.

---

## Customization

### Map style
The map uses Mapbox's `light-v11` style as a base, with paint property overrides applied in `applyMinimalStyle()` inside `index.html`. You can adjust colors by editing that function — or swap the style URL to any Mapbox or custom style.

### Dot colors
- **Dark grey `#1a1a1a`** — venue in dataset, no events found today
- **Red `#c0392b`** — venue has at least one event scraped today
- **Blue `#2962ff`** — your current location

All colors are set via CSS custom properties at the top of the `<style>` block in `index.html`.

### Timezone
The GitHub Actions workflow runs at 12:00 UTC = 7:00 AM EST. In summer (EDT), this is 8:00 AM. To pin to exactly 7am ET year-round, you'd need a slightly more complex setup (check UTC offset dynamically). For most purposes the current setup is fine.

---

## Troubleshooting

**Map doesn't load:**  
Check browser console for errors. Most likely cause: invalid or missing Mapbox token.

**No dots appear:**  
Make sure `venues.json` is in the repo root and properly formatted JSON. Check the browser network tab — `venues.json` should return a 200.

**Events always show "No events listed":**  
Run the scraper manually and check the log. If `events.json` is an empty `{}`, either the scraper ran before your venues had shows today, or the venue sites blocked scraping.

**GitHub Actions workflow not running:**  
Check the Actions tab on your repo. GitHub sometimes disables scheduled workflows on repos with no recent activity — click "Enable workflow" if prompted.

**Geolocation not working:**  
The browser requires HTTPS for `navigator.geolocation`. GitHub Pages serves over HTTPS by default, so this should work. On localhost (`file://` or `http://`) it will silently fail.

---

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

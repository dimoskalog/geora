# GEORA

**Physical economy intelligence.** A live, global map of what is happening to businesses and buildings, built from public government records.

Every dot is a real event (a building permit, a property sale, a new restaurant permit, a health-department closure) with its date, location and the government record it came from. Click a building to see its full timeline.

**Live:** https://dimoskalog.github.io/geora/

## What it does

- 3D globe with country-level data coverage from a 20-country research pass
- New York City: building permits, property sales and mortgages, restaurant permits and inspections, refreshed daily
- Per-building timelines that join events from different agencies through NYC's tax-lot ID (BBL)
- Layers, property-value filter, time window, timeline scrubbing and playback
- "Buildings with a story": properties with two or more kinds of events

## How it works

```
NYC Open Data (DOB permits · ACRIS deeds/mortgages · DOHMH restaurants · PLUTO)
        │  pull_nyc.py  (Python, standard library)
        ▼
normalized events  ─►  history/nyc/first_seen.json   (first day each record appeared)
        │
        ▼
data/nyc/*.json  ─►  index.html  (MapLibre GL globe, vanilla JS)
```

A GitHub Actions workflow runs the pull every morning, saves the history file, and publishes the site with GitHub Pages.

Every event keeps two dates: when it happened, and when it first appeared in public data. The history file is what makes point-in-time questions ("what was known on date X?") possible.

## Run it locally

```
python pull_nyc.py
```
then double-click `index.html`.

## Data sources

All from [NYC Open Data](https://opendata.cityofnewyork.us/) (NYC Open Data Law allows reuse):
DOB NOW: Build – Approved Permits (rbx6-tga4), ACRIS Real Property Master and Legals (bnx9-e6tj, 8h5j-fqxa),
DOHMH Restaurant Inspection Results (43nn-pn8j), PLUTO (64uk-42ks).
Basemap © OpenFreeMap, OpenMapTiles, OpenStreetMap contributors. Country shapes: Natural Earth via world-atlas.

## Roadmap

- [x] NYC v0.1: real events on a 3D globe
- [x] v0.2: layers, filters, timeline, playback
- [ ] v0.3: online, refreshed daily, history saved
- [ ] Paris (SIRENE, BODACC business sales, inspections)
- [ ] Seoul (local permit data)

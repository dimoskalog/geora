# GEORA

**Physical economy intelligence.** A live 3D map of what is happening to businesses and buildings, built entirely from public government records.

Every dot is a real event (a building permit, a property sale, a new business registration, a restaurant inspection, a closure) with its date, location, and the government record it came from. Click a building to see its full timeline.

**Live:** <https://dimoskalog.github.io/geora/>

## At a glance

- **~560,000 events** from the latest daily run (October 2026)
- **56 regions in 15 countries**: 27 U.S. cities and states, 12 U.K. regions, 5 Canadian cities, France, the Netherlands, Ireland, Switzerland, Denmark, Norway, Finland, Estonia, Latvia, Madrid, Singapore, and Melbourne
- **Refreshed every morning** by GitHub Actions, with a full run taking about 30 minutes
- **Live hazard feeds**: USGS earthquakes (M4.5+, past month) and NASA EONET wildfires and severe storms

## What it does

- 3D globe with extruded buildings, built on MapLibre GL
- Layers by event type: new business, fit-out, construction, sale, mortgage, restaurant, distress, and maintenance
- Property-value filter, time window, timeline scrubbing, and playback
- Per-building timelines in New York City that join records from different agencies through the tax-lot ID (BBL)
- "Buildings with a story": properties with two or more kinds of events
- Source-level provenance: every event links back to the public dataset and record it came from

## How it works

```
public open-data APIs (Socrata, ArcGIS Hub, CKAN, national registers)
        │  pipeline/adapters/*.py   one adapter per data family
        ▼
pipeline/core.py   shared event format · permit classification · US geocoding cache
        │
        ├─► history/<region>/first_seen.json   first day each record appeared
        ▼
data/<region>/*.json  ─►  index.html  (MapLibre GL globe, vanilla JS)
```

- **Adapters** (`pipeline/adapters/`) turn each source into one shared event format. Regions run in parallel (8 at a time), since most of the time is spent waiting on the network.
- **Two dates per event**: when it happened, and when it first appeared in public data. The history files make point-in-time questions ("what was known on date X?") possible.
- **Resilient by design**: if a source is down for a day, the pipeline reuses that region's last published data instead of dropping it from the map. Every run writes a per-source status report to `history/last_run.json`.
- **Probe workflow** (`.github/workflows/probe.yml`) checks candidate data URLs before they are added as sources.

## Run it locally

```
python -m pipeline.run                    # all regions
python -m pipeline.run --only us-nyc,fr   # just some regions
```

Then open `index.html`. Python 3.12, standard library only.

Two sources need free API keys, set as environment variables (GitHub Actions secrets in production): `INSEE_KEY` for France's SIRENE register and `COMPANIES_HOUSE_KEY` for the U.K. Companies House. Without them, those sources are skipped and the rest still runs.

## Data sources

All data is public open data. Examples:

- **New York City**: DOB NOW building permits, ACRIS deeds and mortgages, DOHMH restaurant inspections, DCWP licenses, PLUTO
- **Other U.S.**: building permits and business licenses from Chicago, Los Angeles, San Francisco, Philadelphia, Boston, Seattle, Washington DC and more; state business registries for New York, Texas, Colorado, and Connecticut
- **United Kingdom**: FSA Food Hygiene Ratings, Companies House
- **France**: SIRENE, BODACC business sales, Sitadel building permits, Alim'confiance inspections
- **Elsewhere**: Netherlands BAG (Kadaster), Swiss commercial register, Norwegian, Finnish, Estonian, and Latvian business registers, Danish food inspections, Irish planning applications, Singapore ACRA, Madrid planning licenses, Melbourne development activity

Basemap © OpenFreeMap, OpenMapTiles, OpenStreetMap contributors. Country shapes: Natural Earth via world-atlas.

## Roadmap

- [x] v0.1: real NYC events on a 3D globe
- [x] v0.2: layers, filters, timeline, playback
- [x] v0.3: online, refreshed daily, history saved
- [x] v0.4: 56 regions across North America, Europe, and Asia-Pacific
- [ ] Recover sources that currently fail (Houston, Atlanta, Charlotte)
- [ ] Seoul (local permit data)

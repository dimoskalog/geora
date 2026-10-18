"""
US cities on Socrata open-data portals, driven by configuration.

Each dataset lists candidate column names for each role; the adapter reads the dataset's
column list and uses the first candidate that exists, so small schema differences don't break it.
"""
import re
from pipeline.core import socrata, socrata_columns, event, classify_permit, num, title_case, since, point_of, log

DAYS = 90

CITIES = {
    "us-sf": {
        "meta": {"name": "San Francisco", "country": "US", "center": [-122.4194, 37.7749],
                 "bbox": [-122.55, 37.68, -122.33, 37.84]},
        "domain": "data.sfgov.org",
        "datasets": [
            {"id": "i98e-djp9", "source": "SF Building Permits", "mode": "permit",
             "date": ["issued_date", "status_date"], "type": ["permit_type_definition", "permit_type"],
             "desc": ["description"], "cost": ["estimated_cost", "revised_cost"],
             "addr": [["street_number", "street_name", "street_suffix"]], "rid": ["permit_number", "record_id"]},
            {"id": "g8m3-pdis", "source": "SF Registered Business Locations", "mode": "business",
             "date": ["location_start_date"], "name": ["dba_name", "ownership_name"],
             "desc": ["naic_code_description", "lic_code_description"], "addr": ["full_business_address", "street_address"],
             "rid": ["uniqueid", "ttxid", "certificate_number"]},
            {"id": "g8m3-pdis", "source": "SF Registered Business Locations", "mode": "closure",
             "date": ["location_end_date"], "name": ["dba_name", "ownership_name"],
             "desc": ["naic_code_description"], "addr": ["full_business_address", "street_address"],
             "rid": ["uniqueid", "ttxid"]},
        ],
    },
    "us-chicago": {
        "meta": {"name": "Chicago", "country": "US", "center": [-87.6298, 41.8781],
                 "bbox": [-87.95, 41.64, -87.52, 42.03]},
        "domain": "data.cityofchicago.org",
        "datasets": [
            {"id": "ydr8-5enu", "source": "Chicago Building Permits", "mode": "permit",
             "date": ["issue_date"], "type": ["permit_type"], "desc": ["work_description"],
             "cost": ["reported_cost", "estimated_cost"], "addr": [["street_number", "street_direction", "street_name"]],
             "rid": ["permit_", "id", "permit_number"]},
            {"id": "r5kz-chrr", "source": "Chicago Business Licenses", "mode": "business",
             "date": ["license_start_date", "date_issued"], "filter": "application_type='ISSUE'",
             "name": ["doing_business_as_name", "legal_name"], "desc": ["license_description", "business_activity"],
             "addr": ["address"], "rid": ["license_id", "id"]},
            {"id": "4ijn-s7e5", "source": "Chicago Food Inspections", "mode": "inspection",
             "date": ["inspection_date"], "name": ["dba_name", "aka_name"], "desc": ["inspection_type", "facility_type"],
             "result": ["results"], "addr": ["address"], "rid": ["inspection_id"]},
        ],
    },
    "us-seattle": {
        "meta": {"name": "Seattle", "country": "US", "center": [-122.3321, 47.6062],
                 "bbox": [-122.46, 47.48, -122.22, 47.74]},
        "domain": "data.seattle.gov",
        "datasets": [
            {"id": "76t5-zqzr", "source": "Seattle Building Permits", "mode": "permit",
             "date": ["issueddate", "issued_date"], "type": ["permittypemapped", "permitclass", "permittype"],
             "desc": ["description"], "cost": ["estprojectcost"], "addr": ["originaladdress1", "address"],
             "rid": ["permitnum"]},
        ],
    },
    "us-austin": {
        "meta": {"name": "Austin", "country": "US", "center": [-97.7431, 30.2672],
                 "bbox": [-98.05, 30.05, -97.50, 30.55]},
        "domain": "data.austintexas.gov",
        "datasets": [
            {"id": "3syk-w9eu", "source": "Austin Issued Construction Permits", "mode": "permit",
             "date": ["issue_date", "issued_date"], "type": ["permit_type_desc", "work_class", "permittype"],
             "desc": ["description"], "cost": ["total_job_valuation", "total_valuation_remodel"],
             "addr": ["original_address1", "permit_location"], "rid": ["permit_number", "permitnum"]},
        ],
    },
    "us-la": {
        "meta": {"name": "Los Angeles", "country": "US", "center": [-118.2437, 34.0522],
                 "bbox": [-118.70, 33.70, -118.15, 34.34]},
        "domain": "data.lacity.org",
        "datasets": [
            {"id": "pi9x-tg5x", "source": "LADBS Building Permits", "mode": "permit",
             "date": ["issue_date"], "type": ["permit_type", "permit_sub_type"], "desc": ["work_desc", "work_description"],
             "cost": ["valuation"], "addr": [["address_start", "street_direction", "street_name", "street_suffix"], "address"],
             "rid": ["permit_nbr", "pcis_permit"]},
            {"id": "6rrh-rzua", "source": "LA Active Businesses", "mode": "business",
             "date": ["location_start_date"], "name": ["dba_name", "business_name"],
             "desc": ["primary_naics_description", "naics"], "addr": ["street_address"], "rid": ["location_account"]},
        ],
    },
}

ABBR = {"STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD", "ROAD": "RD", "DRIVE": "DR", "PLACE": "PL",
        "LANE": "LN", "COURT": "CT", "PARKWAY": "PKWY", "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
        "TERRACE": "TER", "HIGHWAY": "HWY", "SQUARE": "SQ"}


def norm_addr(a):
    """Location key from an address, so permits and licences at one address share a timeline."""
    a = re.sub(r"[^A-Z0-9 ]", " ", str(a or "").upper())
    a = re.sub(r"\b(SUITE|STE|UNIT|APT|FL|FLOOR|#)\b.*$", "", a)
    return " ".join(ABBR.get(w, w) for w in a.split())[:60] or None


def pick(cols, cands):
    for c in cands:
        if isinstance(c, list):
            if all(x in cols for x in c[:2]):
                return [x for x in c if x in cols]
        elif c in cols:
            return c
    return None


def val(row, col):
    if col is None:
        return None
    if isinstance(col, list):
        return " ".join(str(row.get(c) or "") for c in col).strip()
    return row.get(col)


def dataset_events(city, ds):
    domain, meta = city["domain"], city["meta"]
    cols = socrata_columns(domain, ds["id"])
    r = {k: pick(cols, ds[k]) for k in ("date", "type", "desc", "cost", "addr", "rid", "name", "result") if k in ds}
    if not r.get("date"):
        raise RuntimeError(f"{ds['id']}: no date column among {ds['date']} (has: {', '.join(list(cols)[:40])})")
    point_cols = tuple(c for c, t in cols.items() if t in ("point", "location"))
    where = f"{r['date']} >= '{since(DAYS)}' AND {r['date']} <= '{since(-1)}'"
    if ds.get("filter"):
        where += f" AND {ds['filter']}"
    rows = socrata(domain, ds["id"], where, f"{r['date']} DESC", 40000, label=f"{meta['name']} {ds['source']} ({ds['mode']})")
    out = []
    for row in rows:
        lat, lon = point_of(row, point_keys=point_cols + ("location", "location_1", "the_geom", "point"))
        addr = val(row, r.get("addr"))
        name = title_case(val(row, r.get("name")) or "")
        desc = val(row, r.get("desc")) or ""
        rid = val(row, r.get("rid")) or ""
        when = val(row, r["date"])
        mode = ds["mode"]
        if mode == "permit":
            cost = num(val(row, r.get("cost")))
            kind, sub, title = classify_permit(val(row, r.get("type")), desc, cost)
            if cost and kind != "maintenance":
                title += f" - ${cost:,.0f}"
            amount = cost
        elif mode == "business":
            kind, sub, title, amount = "business", "Business registered", f"{name or 'Business'}: new business location", None
        elif mode == "closure":
            kind, sub, title, amount = "distress", "Business closed", f"{name or 'Business'}: location closed", None
        else:                                   # inspection
            res = str(val(row, r.get("result")) or "")
            if "OUT OF BUSINESS" in res.upper():
                kind, sub, title = "distress", "Out of business", f"{name}: found out of business"
            elif res.upper().startswith("FAIL"):
                kind, sub, title = "restaurant", "Failed inspection", f"{name}: failed inspection"
            else:
                kind, sub, title = "restaurant", "Inspection", f"{name}: inspected - {res.lower() or 'result n/a'}"
            amount = None
        out.append(event(kind, sub, when, lat, lon, title, desc, ds["source"], rid, norm_addr(addr), addr, amount, meta["bbox"]))
    got = [e for e in out if e]
    log(f"   -> {len(got)} located events of {len(rows)} rows")
    return got


def regions():
    res = []
    for rid, city in CITIES.items():
        meta = dict(city["meta"], sources=sorted({d["source"] for d in city["datasets"]}))
        fns = [(lambda c=city, d=d: dataset_events(c, d)) for d in city["datasets"]]
        for f, d in zip(fns, city["datasets"]):
            f.__name__ = f"{d['source']} ({d['mode']})"
        res.append((rid, meta, fns))
    return res

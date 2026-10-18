"""
US cities on Socrata open-data portals, driven by configuration.

Each dataset lists candidate column names for each role; the adapter reads the dataset's
column list and uses the first candidate that exists, so small schema differences don't break it.
"""
import re
from pipeline.core import socrata, socrata_columns, http, event, classify_permit, num, title_case, since, point_of, log

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

# Many US cities publish permits in the BLDS standard; these are the usual column names.
BLDS = {"date": ["issueddate", "issued_date", "issue_date", "dateissued", "permit_issued_date", "issuedate"],
        "type": ["permittypemapped", "permittype", "permit_type", "permitclassmapped", "permitclass", "type", "worktype"],
        "desc": ["description", "workdescription", "work_description", "projectdescription", "permit_description"],
        "cost": ["estprojectcost", "estimated_cost", "valuation", "projectvalue", "value", "constrval", "job_value", "declared_valuation"],
        "addr": ["originaladdress1", "address", "original_address1", "streetaddress", "street_address", "location_address", "fulladdress"],
        "rid": ["permitnum", "permit_number", "permitnumber", "permit_no", "permit_", "id", "numstring"]}


def city(name, domain, center, bbox, search="building permits", must=("permit",)):
    return {"meta": {"name": name, "country": "US", "center": center, "bbox": bbox}, "domain": domain,
            "datasets": [dict(BLDS, search=search, must=must, source=f"{name} Building Permits", mode="permit")]}


CITIES.update({
    "us-dallas": city("Dallas", "www.dallasopendata.com", [-96.797, 32.7767], [-97.0, 32.62, -96.55, 33.03]),
    "us-nola": city("New Orleans", "data.nola.gov", [-90.0715, 29.9511], [-90.14, 29.86, -89.62, 30.20]),
    "us-kc": city("Kansas City", "data.kcmo.org", [-94.5786, 39.0997], [-94.77, 38.85, -94.38, 39.36]),
    "us-cincinnati": city("Cincinnati", "data.cincinnati-oh.gov", [-84.512, 39.1031], [-84.72, 39.05, -84.36, 39.23]),
    "us-montgomery": city("Montgomery County, MD", "data.montgomerycountymd.gov", [-77.152, 39.084], [-77.53, 38.93, -76.88, 39.36]),
    "us-cambridge": city("Cambridge, MA", "data.cambridgema.gov", [-71.1097, 42.3736], [-71.17, 42.35, -71.06, 42.41]),
    "us-nashville": city("Nashville", "data.nashville.gov", [-86.7816, 36.1627], [-87.06, 35.96, -86.51, 36.41]),
    "us-honolulu": city("Honolulu", "data.honolulu.gov", [-157.8583, 21.3069], [-158.3, 21.25, -157.6, 21.72]),
    "us-mesa": city("Mesa, AZ", "data.mesaaz.gov", [-111.8315, 33.4152], [-111.95, 33.27, -111.57, 33.51]),
    "us-baton-rouge": city("Baton Rouge", "data.brla.gov", [-91.1403, 30.4515], [-91.3, 30.3, -90.95, 30.6]),
})

ABBR = {"STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD", "ROAD": "RD", "DRIVE": "DR", "PLACE": "PL",
        "LANE": "LN", "COURT": "CT", "PARKWAY": "PKWY", "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
        "TERRACE": "TER", "HIGHWAY": "HWY", "SQUARE": "SQ"}


def iso_date(v):
    """'2026-10-01T00:00:00.000', '10/01/2026', '2026/10/01', epoch ms -> '2026-10-01'."""
    s = str(v or "").strip()
    m = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        return f"{m[1]}-{int(m[2]):02d}-{int(m[3]):02d}"
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{m[3]}-{int(m[1]):02d}-{int(m[2]):02d}"
    if s.isdigit() and len(s) >= 12:
        import datetime
        return datetime.datetime.utcfromtimestamp(int(s) / 1000).strftime("%Y-%m-%d")
    return None


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


def candidates(domain, ds):
    if ds.get("id"):
        yield ds["id"], None
    if ds.get("search"):
        res = http("https://api.us.socrata.com/api/catalog/v1",
                   {"domains": domain, "q": ds["search"], "only": "dataset", "limit": 25}).get("results", [])
        res.sort(key=lambda x: x["resource"].get("data_updated_at") or "", reverse=True)
        for x in res:
            nm = x["resource"]["name"].lower()
            if all(m in nm for m in ds.get("must", ())) and not any(b in nm for b in ("inspection", "map", "summary", "count", "fee")):
                yield x["resource"]["id"], x["resource"]["name"]


def dataset_events(city, ds):
    domain, meta = city["domain"], city["meta"]
    tried = []
    for dsid, dsname in candidates(domain, ds):
        try:
            cols = socrata_columns(domain, dsid)
        except RuntimeError as e:
            tried.append(f"{dsid}: {str(e)[:80]}")
            continue
        r = {k: pick(cols, ds[k]) for k in ("date", "type", "desc", "cost", "addr", "rid", "name", "result") if k in ds}
        if r.get("date"):
            ds = dict(ds, id=dsid)
            if dsname:
                log(f"   {meta['name']}: using dataset {dsid} '{dsname}'")
            break
        tried.append(f"{dsid} '{dsname}': no date column (has {', '.join(list(cols)[:25])})")
    else:
        raise RuntimeError("no usable dataset; tried " + " | ".join(tried[:6]))
    point_cols = tuple(c for c, t in cols.items() if t in ("point", "location"))
    real_date = cols.get(r["date"]) in ("calendar_date", "floating_timestamp", "date", "fixed_timestamp")
    if real_date:
        where, order = f"{r['date']} >= '{since(DAYS)}' AND {r['date']} <= '{since(-1)}'", f"{r['date']} DESC"
    else:                                      # date stored as text: newest rows first, filter in Python
        where, order = "1=1", ":id DESC"
    if ds.get("filter"):
        where += f" AND {ds['filter']}"
    rows = socrata(domain, ds["id"], where, order, 40000, label=f"{meta['name']} {ds['source']} ({ds['mode']})")
    out = []
    for row in rows:
        lat, lon = point_of(row, point_keys=point_cols + ("location", "location_1", "the_geom", "point"))
        addr = val(row, r.get("addr"))
        name = title_case(val(row, r.get("name")) or "")
        desc = val(row, r.get("desc")) or ""
        rid = val(row, r.get("rid")) or ""
        when = iso_date(val(row, r["date"]))
        if not when or not (since(DAYS) <= when <= since(-1)):
            continue
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

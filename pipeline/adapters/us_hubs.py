"""
US cities on ArcGIS Hub and CKAN portals.

ArcGIS Hub: read the site's public DCAT catalog (/api/feed/dcat-us/1.1.json), pick the building-permit
layer that has recent records, and query its FeatureServer as GeoJSON.
CKAN: search packages, pick a datastore resource with recent records, query it with SQL.
Rows without coordinates are geocoded with the US Census geocoder.
"""
import re
from pipeline.core import http, event, classify_permit, num, since, log, geocode_us
from pipeline.adapters.us_cities import BLDS, norm_addr, iso_date

DAYS = 90
BAD = ("inspection", "summary", "count", "fee", "fire", "tent", "sign", "electrical", "plumbing", "mechanical",
       "right of way", "right-of-way", "row ", "street", "sidewalk", "special event", "film", "historic", "parking",
       "dumpster", "tree", "excavation", "encroachment", "boundary", "district", "zoning", "map", "dashboard")
HUBS = {  # region: (name, state, hub domain, center)
    "us-dc": ("Washington, DC", "DC", "opendata.dc.gov", [-77.0369, 38.9072]),
    "us-detroit": ("Detroit", "MI", "data.detroitmi.gov", [-83.0458, 42.3314]),
    "us-baltimore": ("Baltimore", "MD", "data.baltimorecity.gov", [-76.6122, 39.2904]),
    "us-minneapolis": ("Minneapolis", "MN", "opendata.minneapolismn.gov", [-93.265, 44.9778]),
    "us-raleigh": ("Raleigh", "NC", "data-ral.opendata.arcgis.com", [-78.6382, 35.7796]),
    "us-charlotte": ("Charlotte", "NC", "data.charlottenc.gov", [-80.8431, 35.2271]),
    "us-columbus": ("Columbus", "OH", "opendata.columbus.gov", [-82.9988, 39.9612]),
    "us-miami": ("Miami-Dade", "FL", "gis-mdc.opendata.arcgis.com", [-80.1918, 25.7617]),
    "us-houston": ("Houston", "TX", "cohgis-mycity.opendata.arcgis.com", [-95.3698, 29.7604]),
    "us-atlanta": ("Atlanta", "GA", "dpcd-coaplangis.opendata.arcgis.com", [-84.388, 33.749]),
    "us-vegas": ("Las Vegas", "NV", "opendataportal-lasvegas.opendata.arcgis.com", [-115.1398, 36.1699]),
    "us-tampa": ("Tampa", "FL", "city-tampa.opendata.arcgis.com", [-82.4572, 27.9506]),
    "us-denver": ("Denver", "CO", "opendata-geospatialdenver.hub.arcgis.com", [-104.9903, 39.7392]),
}
CKANS = {
    "us-sanantonio": ("San Antonio", "TX", "data.sanantonio.gov", [-98.4936, 29.4241]),
    "us-pittsburgh": ("Pittsburgh", "PA", "data.wprdc.org", [-79.9959, 40.4406]),
    "us-phoenix": ("Phoenix", "AZ", "www.phoenixopendata.com", [-112.074, 33.4484]),
    "us-sanjose": ("San Jose", "CA", "data.sanjoseca.gov", [-121.8863, 37.3382]),
}


def bbox_of(c, dx=0.45, dy=0.35):
    return [c[0] - dx, c[1] - dy, c[0] + dx, c[1] + dy]


def ci_pick(keys, cands):
    low = {k.lower(): k for k in keys}
    for c in cands:
        if c.lower() in low:
            return low[c.lower()]
    return None


def roles(keys):
    return {k: ci_pick(keys, v) for k, v in BLDS.items() if k != "date"}


def to_events(rows, r, name, state, center, source, pending_ok=True):
    """rows: list of (props, lat, lon, date_iso)."""
    bbox = bbox_of(center, 0.8, 0.6)
    out, pending = [], []
    for p, lat, lon, when in rows:
        if not when or not (since(DAYS) <= when <= since(-1)):
            continue
        g = lambda k: p.get(r.get(k)) if r.get(k) else None
        cost = num(g("cost"))
        kind, sub, title = classify_permit(g("type"), g("desc"), cost)
        if cost and kind != "maintenance":
            title += f" - ${cost:,.0f}"
        addr = str(g("addr") or "").strip()
        args = [kind, sub, when, lat, lon, title, g("desc"), source, g("rid"), norm_addr(addr), addr, cost, bbox]
        if lat is None and addr and pending_ok:
            pending.append(args)
        else:
            out.append(event(*args))
    if pending:
        log(f"   {len(pending)} rows without coordinates: geocoding")
        geo = geocode_us([(str(i), a[10], name.split(",")[0].split("-")[0], state, "") for i, a in enumerate(pending)])
        for i, a in enumerate(pending):
            if str(i) in geo:
                a[3], a[4] = geo[str(i)]
                out.append(event(*a))
    return [e for e in out if e]


# -------------------------------------------------------------- ArcGIS Hub ----
def hub_layers(domain):
    cat = http(f"https://{domain}/api/feed/dcat-us/1.1.json", timeout=120)
    found = []
    for d in cat.get("dataset", []):
        t = (d.get("title") or "").lower()
        if "permit" not in t or any(b in t for b in BAD):
            continue
        for dist in d.get("distribution", []):
            u = dist.get("accessURL") or dist.get("downloadURL") or ""
            if re.search(r"/(FeatureServer|MapServer)(/\d+)?/?$", u):
                if not re.search(r"/\d+/?$", u):
                    u = u.rstrip("/") + "/0"
                found.append((("building" in t, d.get("modified") or ""), u.rstrip("/"), d.get("title")))
                break
    found.sort(reverse=True)
    return [(u, t) for _, u, t in found[:8]]


def hub_city(rid):
    name, state, domain, center = HUBS[rid]
    log(f"\n== {name} (ArcGIS Hub {domain})")
    tried = []
    for url, title in hub_layers(domain):
        try:
            meta = http(url, {"f": "json"}, tries=2)
        except RuntimeError as e:
            tried.append(f"{title}: {str(e)[:60]}")
            continue
        fields = meta.get("fields") or []
        dates = [f["name"] for f in fields if f.get("type") in ("esriFieldTypeDate", "esriFieldTypeDateOnly")]
        df = next((d for d in dates if re.search(r"issu", d, re.I)), None) or next(iter(dates), None)
        if not df:
            tried.append(f"{title}: no date field")
            continue
        where = f"{df} >= DATE '{since(DAYS)}'"
        try:
            n = http(f"{url}/query", {"where": where, "returnCountOnly": "true", "f": "json"}, tries=2).get("count", 0)
        except RuntimeError:
            where = f"{df} >= timestamp '{since(DAYS)} 00:00:00'"
            try:
                n = http(f"{url}/query", {"where": where, "returnCountOnly": "true", "f": "json"}, tries=2).get("count", 0)
            except RuntimeError as e:
                tried.append(f"{title}: query failed {str(e)[:60]}")
                continue
        if not n:
            tried.append(f"{title}: no recent records")
            continue
        log(f"   using '{title}' ({n} records since {since(DAYS)}) {url}")
        rows, off, step = [], 0, min(2000, meta.get("maxRecordCount") or 1000)
        while off < min(n, 30000):
            page = http(f"{url}/query", {"where": where, "outFields": "*", "outSR": 4326, "f": "geojson",
                                         "orderByFields": f"{df} DESC", "resultOffset": off, "resultRecordCount": step}, timeout=300)
            feats = page.get("features", [])
            for f in feats:
                p = f.get("properties") or {}
                g = f.get("geometry") or {}
                c = g.get("coordinates")
                while isinstance(c, list) and c and isinstance(c[0], list):
                    c = c[0]
                lat, lon = (num(c[1]), num(c[0])) if isinstance(c, list) and len(c) >= 2 else (None, None)
                rows.append((p, lat, lon, iso_date(p.get(df))))
            if len(feats) < step:
                break
            off += len(feats)
        r = roles(rows[0][0].keys()) if rows else {}
        return to_events(rows, r, name, state, center, f"{name} {title}")
    raise RuntimeError("no usable permit layer; tried " + " | ".join(tried[:6]))


# -------------------------------------------------------------------- CKAN ----
def ckan_city(rid):
    name, state, domain, center = CKANS[rid]
    log(f"\n== {name} (CKAN {domain})")
    api = f"https://{domain}/api/3/action"
    pk = http(f"{api}/package_search", {"q": "building permits", "rows": 25}, timeout=120)["result"]["results"]
    res = []
    for p in pk:
        t = (p.get("title") or "").lower()
        if "permit" not in t or any(b in t for b in BAD):
            continue
        for rsc in p.get("resources", []):
            if rsc.get("datastore_active"):
                res.append((("building" in t, rsc.get("last_modified") or p.get("metadata_modified") or ""), rsc["id"], f"{p['title']} / {rsc.get('name')}"))
    res.sort(reverse=True)
    tried = []
    for _, rsid, title in res[:8]:
        try:
            fields = http(f"{api}/datastore_search", {"resource_id": rsid, "limit": 1}, tries=2)["result"]["fields"]
        except (RuntimeError, KeyError) as e:
            tried.append(f"{title}: {str(e)[:60]}")
            continue
        names = [f["id"] for f in fields]
        dates = [f["id"] for f in fields if str(f.get("type")).startswith(("timestamp", "date"))]
        df = next((d for d in dates if re.search(r"issu", d, re.I)), None) or next(iter(dates), None) \
            or next((n for n in names if re.search(r"issue.*date|date.*issue", n, re.I)), None)
        if not df:
            tried.append(f"{title}: no date field")
            continue
        sql = f'SELECT * FROM "{rsid}" WHERE "{df}" >= \'{since(DAYS)}\' ORDER BY "{df}" DESC LIMIT 30000'
        try:
            recs = http(f"{api}/datastore_search_sql", {"sql": sql}, timeout=300)["result"]["records"]
        except (RuntimeError, KeyError) as e:
            tried.append(f"{title}: sql failed {str(e)[:60]}")
            continue
        if not recs:
            tried.append(f"{title}: no recent records")
            continue
        log(f"   using '{title}' ({len(recs)} records)")
        latk = ci_pick(names, ["latitude", "lat", "y", "gpsy", "y_latitude"])
        lonk = ci_pick(names, ["longitude", "lon", "lng", "long", "x", "gpsx", "x_longitude"])
        rows = [(p, num(p.get(latk)) if latk else None, num(p.get(lonk)) if lonk else None, iso_date(p.get(df))) for p in recs]
        return to_events(rows, roles(names), name, state, center, f"{name} {title.split(' / ')[0]}")
    raise RuntimeError("no usable permit resource; tried " + " | ".join(tried[:6]))


def regions():
    out = []
    for rid, (name, state, dom, c) in HUBS.items():
        out.append((rid, {"name": name, "country": "US", "state": state, "center": c, "bbox": bbox_of(c, 0.8, 0.6),
                          "sources": [f"{name} open data (ArcGIS Hub)"]}, [lambda r=rid: hub_city(r)]))
    for rid, (name, state, dom, c) in CKANS.items():
        out.append((rid, {"name": name, "country": "US", "state": state, "center": c, "bbox": bbox_of(c, 0.8, 0.6),
                          "sources": [f"{name} open data (CKAN)"]}, [lambda r=rid: ckan_city(r)]))
    for _, _, fns in out:
        fns[0].__name__ = "permits"
    return out

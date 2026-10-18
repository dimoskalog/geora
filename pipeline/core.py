"""
GEORA pipeline core: HTTP, the shared event format, history and output.

Every adapter returns a list of events (GeoJSON features) built with `event()`.
An event always carries:
  kind      one of the map layers (business, fitout, construction, sale, mortgage,
            restaurant, distress, maintenance)
  date      when it happened (as the source says)
  seen      first day GEORA saw it in public data (from history/)
  loc       stable location key inside its region (tax lot, permit location, address...)
  source / rid   which public dataset and which record, so every dot has evidence
"""
import json, os, re, ssl, time, urllib.parse, urllib.request
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
HISTORY = os.path.join(ROOT, "history")
UA = "GEORA/0.4 (+https://github.com/dimoskalog/geora)"
TODAY = date.today()

try:                                   # Windows certificate fix (local runs)
    import truststore
    truststore.inject_into_ssl()
except Exception:
    try:
        import certifi
        ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
    except Exception:
        pass


def since(days):
    return (TODAY - timedelta(days=days)).isoformat()


# ------------------------------------------------------------------ HTTP ----
def http(url, params=None, data=None, headers=None, tries=4, timeout=180, raw=False):
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    h = {"User-Agent": UA, "Accept": "application/json"}
    h.update(headers or {})
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
                return body if raw else json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}: {e.read()[:300].decode('utf-8', 'replace')}"
            if e.code in (400, 401, 403, 404):
                break
        except Exception as e:
            last = repr(e)
        time.sleep(3 * (i + 1))
    raise RuntimeError(f"{url[:200]} -> {last}")


# --------------------------------------------------------------- Socrata ----
def socrata_columns(domain, ds):
    meta = http(f"https://{domain}/api/views/{ds}.json")
    return {c["fieldName"]: c.get("dataTypeName") for c in meta.get("columns", []) if c.get("fieldName")}


def socrata(domain, ds, where, order, cap, select=None, label=""):
    rows, off = [], 0
    while len(rows) < cap:
        p = {"$where": where, "$order": order, "$limit": min(50000, cap - len(rows)), "$offset": off}
        if select:
            p["$select"] = select
        batch = http(f"https://{domain}/resource/{ds}.json", p)
        rows += batch
        log(f"   {label}: {len(rows)}")
        if len(batch) < p["$limit"]:
            break
        off += len(batch)
    return rows


# ------------------------------------------------------------- utilities ----
LOG = []


def log(msg):
    print(msg, flush=True)
    LOG.append(str(msg))


def num(x):
    try:
        v = float(str(x).replace(",", "").replace("$", ""))
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def clean(s, n=240):
    s = re.sub(r"\s+", " ", str(s or "")).strip()
    return s[:n]


def title_case(s):
    return " ".join(w.capitalize() if w.isupper() or w.islower() else w for w in str(s or "").split())


def point_of(row, lat_keys=("latitude", "lat"), lon_keys=("longitude", "lon", "long"), point_keys=("location", "the_geom", "point", "geocoded_column", "business_location")):
    """Find coordinates in a Socrata row: separate lat/lon columns or a point column."""
    for la in lat_keys:
        for lo in lon_keys:
            if row.get(la) not in (None, "") and row.get(lo) not in (None, ""):
                return num(row[la]), num(row[lo])
    for k in point_keys:
        v = row.get(k)
        if isinstance(v, dict):
            if v.get("type") == "Point" and v.get("coordinates"):
                return num(v["coordinates"][1]), num(v["coordinates"][0])
            if v.get("latitude") and v.get("longitude"):
                return num(v["latitude"]), num(v["longitude"])
    return None, None


def event(kind, sub, when, lat, lon, title, detail, source, rid, loc=None, addr=None, amount=None, bbox=None):
    if lat is None or lon is None or (lat == 0 and lon == 0):
        return None
    if bbox and not (bbox[1] <= lat <= bbox[3] and bbox[0] <= lon <= bbox[2]):
        return None
    p = {"kind": kind, "sub": sub, "date": str(when or "")[:10], "title": clean(title, 160),
         "detail": clean(detail), "source": source, "rid": str(rid or ""), "loc": loc, "addr": clean(addr, 120),
         "amount": amount}
    if not re.match(r"\d{4}-\d{2}-\d{2}$", p["date"]):
        return None
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
            "properties": {k: v for k, v in p.items() if v not in (None, "")}}


# ---------------------------------------------------- permit classification ----
MINOR_WORK = ("SIDEWALK SHED", "SCAFFOLD", "CONSTRUCTION FENCE", "PROTECTION", "MECHANICAL", "PLUMBING",
              "SPRINKLER", "STANDPIPE", "BOILER", "ANTENNA", "SOLAR", "CURB CUT", "SIGN", "GAS", "ELECTRICAL",
              "ELEVATOR", "FIRE ALARM", "HVAC", "REROOF", "RE-ROOF", "WATER HEATER", "FENCE", "SIDE SEWER")
STOREFRONT = ("STORE", "RETAIL", "RESTAURANT", "EATING", "CAFE", "TENANT", "FIT-OUT", "FIT OUT", "SALON", "GYM",
              "BAR ", "COMMERCIAL SPACE", "CHANGE OF USE", "CHANGE IN USE", "OFFICE SPACE", "SHOWROOM", "BANK",
              "TI ", "T.I.", "TENANT IMPROVEMENT")


def classify_permit(ptype, desc, cost):
    """Sort a permit into: new building / demolition / storefront / major renovation / minor work."""
    w, d = str(ptype or "").upper(), f" {str(desc or '').upper()} "
    if "DEMOLI" in w or "DEMOLISH" in d or "DEMOLITION OF" in d or "WRECK" in w:
        return "construction", "Demolition", "Demolition permit"
    if "NEW BUILDING" in d or "NEW CONSTRUCTION" in w or "NEW BUILDING" in w or " NB " in d or "ERECT" in d \
            or "NEW CONSTRUCTION" in d or w in ("FOUNDATION", "EARTHWORK"):
        return "construction", "New building", "New building permit"
    if any(k in w for k in MINOR_WORK):
        return "maintenance", title_case(ptype), f"{title_case(ptype)} permit"
    if any(k in d for k in STOREFRONT):
        return "fitout", "Storefront / interior", "Storefront or interior work permit"
    if cost and cost >= 1_000_000:
        return "construction", "Major renovation", "Major renovation permit"
    return "maintenance", title_case(ptype) or "Building work", f"{title_case(ptype) or 'Building work'} permit"


# ----------------------------------------------------------------- history ----
def apply_history(region, feats):
    """Remember the first day each record appeared; undated 'new' records get that day as their date."""
    path = os.path.join(HISTORY, region, "first_seen.json")
    try:
        with open(path, encoding="utf-8") as f:
            seen = json.load(f)
    except (OSError, ValueError):
        seen = {}
    today, new = TODAY.isoformat(), 0
    for ft in feats:
        p = ft["properties"]
        undated = p.pop("undated", False)
        key = f"{p['kind']}|{p['source']}|{p['rid']}|{'' if undated else p['date']}"
        if key not in seen:
            seen[key] = today
            new += 1
        p["seen"] = seen[key]
        if undated:
            p["date"] = seen[key]
    cutoff = since(400)
    seen = {k: v for k, v in seen.items() if v >= cutoff}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(seen, f, separators=(",", ":"), sort_keys=True)
    log(f"   history: {new} records seen for the first time today ({len(seen)} remembered)")


def write_region(region, meta, feats):
    """data/<region>/<kind>.json + manifest.json; returns the manifest."""
    out = os.path.join(DATA, region)
    os.makedirs(out, exist_ok=True)
    feats.sort(key=lambda f: f["properties"]["date"])
    by_kind = {}
    for f in feats:
        by_kind.setdefault(f["properties"]["kind"], []).append(f)
    files = []
    for kind, lst in sorted(by_kind.items()):
        with open(os.path.join(out, f"{kind}.json"), "w", encoding="utf-8") as fh:
            json.dump({"type": "FeatureCollection", "features": lst}, fh, separators=(",", ":"))
        files.append({"kind": kind, "file": f"{kind}.json", "count": len(lst)})
    dates = [f["properties"]["date"] for f in feats]
    manifest = dict(meta, id=region, generated=time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
                    events=len(feats), first=min(dates) if dates else None, last=max(dates) if dates else None,
                    files=files)
    with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=1)
    return manifest

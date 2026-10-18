"""
NYC data pull: turns NYC government records into dated, located events for the map.
Standard library only.

  python pull_nyc.py          local run: writes data/nyc/*.json and data/events.js (double-click index.html)
  python pull_nyc.py --site   website build (GitHub Actions): writes data/nyc/*.json only

history/nyc/first_seen.json remembers the first day each record appeared in the city's data.
That history is kept in the repository, so it grows every day the update runs.
Sources (all NYC Open Data, no key needed):
  DOB NOW: Build - Approved Permits   rbx6-tga4   construction
  DOHMH Restaurant Inspections        43nn-pn8j   restaurants (new + inspected)
  ACRIS Real Property Master/Legals   bnx9-e6tj / 8h5j-fqxa   deeds + mortgages
  PLUTO (tax lots)                    64uk-42ks   coordinates for ACRIS properties
"""
import json, os, ssl, time, urllib.parse, urllib.request
from datetime import date, timedelta

DAYS = 90
SINCE = (date.today() - timedelta(days=DAYS)).isoformat()
# ACRIS (property records) is published on Open Data ~2-3 months late, so look back further
ACRIS_SINCE = (date.today() - timedelta(days=180)).isoformat()
CAP = {"permits": 80000, "restaurants": 60000, "acris": 30000}
DOMAIN = "data.cityofnewyork.us"
UA = "geora-nyc/0.3 (+https://github.com/dimkalo/geora)"
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data")

try:                                   # same certificate fix as slice_probe.py
    import truststore
    truststore.inject_into_ssl()
except Exception:
    try:
        import certifi
        ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
    except Exception:
        pass

BORO = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island",
        "MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3", "QUEENS": "4", "STATEN ISLAND": "5"}


def get(path, params=None, tries=4):
    url = f"https://{DOMAIN}{path}" + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            msg = e.read()[:300].decode("utf-8", "replace")
            if e.code in (400, 404):
                raise RuntimeError(f"HTTP {e.code} {url}\n{msg}")
            print(f"   retry {i+1}: HTTP {e.code}")
        except Exception as e:
            print(f"   retry {i+1}: {e}")
        time.sleep(3 * (i + 1))
    raise RuntimeError(f"failed: {url}")


def pull(ds, where, order, cap, select=None, label=""):
    rows, off, page = [], 0, 50000
    while len(rows) < cap:
        p = {"$where": where, "$order": order, "$limit": min(page, cap - len(rows)), "$offset": off}
        if select:
            p["$select"] = select
        batch = get(f"/resource/{ds}.json", p)
        rows += batch
        print(f"   {label}: {len(rows)}")
        if len(batch) < p["$limit"]:
            break
        off += len(batch)
    return rows


def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def bbl_of(boro, block, lot):
    b = str(boro).strip().upper()
    b = b if b.isdigit() else BORO.get(b, b)          # "1".."5" stay as is; "MANHATTAN" -> "1"
    try:
        return f"{int(b)}{int(block):05d}{int(lot):04d}"
    except (TypeError, ValueError):
        return None


def clean_bbl(x):
    if not x:
        return None
    s = str(x).split(".")[0]
    return s if len(s) == 10 and s.isdigit() else None


def ev(kind, sub, d, lat, lon, title, detail, source, record_id, bbl=None, address=None, amount=None):
    if lat is None or lon is None or not (40.4 < lat < 41.0 and -74.3 < lon < -73.6):
        return None
    return {"type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
            "properties": {"country": "US", "city": "New York", "kind": kind, "sub": sub, "date": d[:10], "title": title, "detail": detail,
                           "source": source, "rid": record_id, "bbl": bbl, "addr": address,
                           "amount": amount}}


# ------------------------------------------------------------ construction ----
MINOR_WORK = ("SIDEWALK SHED", "SCAFFOLD", "CONSTRUCTION FENCE", "PROTECTION", "MECHANICAL", "PLUMBING",
              "SPRINKLER", "STANDPIPE", "BOILER", "ANTENNA", "SOLAR", "CURB CUT", "SIGN", "GAS", "ELECTRICAL")
STOREFRONT = ("STORE", "RETAIL", "RESTAURANT", "EATING", "CAFE", "TENANT", "FIT-OUT", "FIT OUT", "SALON", "GYM",
              "BAR ", "COMMERCIAL SPACE", "CHANGE OF USE", "CHANGE IN USE", "OFFICE SPACE", "SHOWROOM", "BANK")


def classify_permit(wt, desc, cost):
    """Sort a permit into: new building / demolition / storefront / major renovation / minor work."""
    w, d = wt.upper(), desc.upper()
    if "DEMOLITION" in w or "DEMOLISH" in d or "DEMOLITION OF" in d:
        return "construction", "Demolition", "Demolition permit"
    if "NEW BUILDING" in d or " NB " in f" {d} " or "NB APPLICATION" in d or w in ("FOUNDATION", "EARTHWORK"):
        return "construction", "New building", "New building permit" if w not in ("FOUNDATION", "EARTHWORK") else f"{wt} permit (new building)"
    if any(k in w for k in MINOR_WORK):
        return "maintenance", wt, f"{wt} permit"
    if any(k in d for k in STOREFRONT):
        return "fitout", "Storefront / interior", "Storefront or interior work permit"
    if cost and cost >= 1_000_000:
        return "construction", "Major renovation", "Major renovation permit"
    return "maintenance", wt, f"{wt} permit"


def permits():
    print("\n== DOB NOW permits")
    rows = pull("rbx6-tga4", f"issued_date >= '{SINCE}'", "issued_date DESC", CAP["permits"], label="permits")
    out = []
    for r in rows:
        addr = f"{r.get('house_no', '')} {r.get('street_name', '')}".strip()
        cost = fnum(r.get("estimated_job_costs"))
        wt = (r.get("work_type") or "Work").title()
        desc = (r.get("job_description") or "").strip()
        kind, sub, title = classify_permit(wt, desc, cost)
        if cost and kind != "maintenance":
            title += f" - ${cost:,.0f}"
        out.append(ev(kind, sub, r.get("issued_date", ""), fnum(r.get("latitude")), fnum(r.get("longitude")),
                      title, desc[:240], "NYC DOB NOW: Build", r.get("work_permit") or r.get("job_filing_number"),
                      clean_bbl(r.get("bbl")) or bbl_of(r.get("borough"), r.get("block"), r.get("lot")), addr, cost))
    out = [e for e in out if e]
    from collections import Counter
    print("   permit types:", dict(Counter(e["properties"]["kind"] for e in out)))
    return out


# ------------------------------------------------------------- restaurants ----
def restaurants():
    print("\n== Restaurant inspections")
    sel = "camis,dba,boro,building,street,cuisine_description,inspection_date,inspection_type,action,grade,score,latitude,longitude,bbl"
    rows = pull("43nn-pn8j", f"inspection_date >= '{SINCE}'", "inspection_date DESC", CAP["restaurants"], sel, "inspection rows")
    print("   + restaurants with a permit but no inspection yet (new)")
    new = pull("43nn-pn8j", "inspection_date < '1901-01-01'", "record_date DESC", 20000,
               sel + ",record_date", "new restaurant rows")
    seen, out = set(), []
    for r in rows:                                  # one row per violation -> one event per visit
        k = (r.get("camis"), r.get("inspection_date"))
        if k in seen:
            continue
        seen.add(k)
        g = r.get("grade")
        it = r.get("inspection_type") or "Inspection"
        closed = "closed" in (r.get("action") or "").lower()
        title = ("Closed by health department" if closed else
                 f"Inspected{' - grade ' + g if g in ('A', 'B', 'C') else ''}")
        out.append(ev("distress" if closed else "restaurant", "Closure order" if closed else "Inspection",
                      r.get("inspection_date", ""), fnum(r.get("latitude")), fnum(r.get("longitude")),
                      f"{(r.get('dba') or '').title()}: {title}", f"{it}. {r.get('cuisine_description') or ''}".strip(),
                      "NYC DOHMH inspections", r.get("camis"), clean_bbl(r.get("bbl")),
                      f"{r.get('building', '')} {r.get('street', '')}".strip()))
    seen = set()
    for r in new:
        if r.get("camis") in seen:
            continue
        seen.add(r.get("camis"))
        out.append(ev("business", "New restaurant permit", r.get("record_date", ""),
                      fnum(r.get("latitude")), fnum(r.get("longitude")),
                      f"{(r.get('dba') or '').title()}: new restaurant, not yet inspected",
                      f"{r.get('cuisine_description') or ''}. Date shown is the first day this permit appeared in the city's data.",
                      "NYC DOHMH inspections", r.get("camis"), clean_bbl(r.get("bbl")),
                      f"{r.get('building', '')} {r.get('street', '')}".strip()))
    return [e for e in out if e]


# ------------------------------------------------------------------ property ----
def acris():
    print("\n== ACRIS deeds and mortgages")
    print(f"   window from {ACRIS_SINCE} (the city publishes these with a delay)")
    master = pull("bnx9-e6tj", f"recorded_datetime >= '{ACRIS_SINCE}' AND doc_type in ('DEED','MTGE')",
                  "recorded_datetime DESC", CAP["acris"],
                  "document_id,doc_type,document_amt,recorded_datetime,document_date", "documents")
    docs = {m["document_id"]: m for m in master}
    ids = list(docs)
    legals = []
    for i in range(0, len(ids), 150):
        chunk = ",".join(f"'{x}'" for x in ids[i:i + 150])
        legals += get("/resource/8h5j-fqxa.json", {"$where": f"document_id in ({chunk})", "$limit": 5000,
                      "$select": "document_id,borough,block,lot,street_number,street_name,unit,property_type"})
        if i % 3000 == 0:
            print(f"   legals: {len(legals)} (docs {i}/{len(ids)})")
    # coordinates from PLUTO by tax lot
    meta = get("/api/views/64uk-42ks.json")
    cols = {c["fieldName"]: c.get("dataTypeName") for c in meta.get("columns", [])}
    bblcol = "bbl" if "bbl" in cols else None
    latc = next((c for c in ("latitude", "lat") if c in cols), None)
    lonc = next((c for c in ("longitude", "lon", "long") if c in cols), None)
    print(f"   PLUTO columns used: {bblcol}, {latc}, {lonc} (bbl type {cols.get(bblcol)})")
    need = sorted({b for b in (bbl_of(l.get("borough"), l.get("block"), l.get("lot")) for l in legals) if b})
    coords = {}
    if bblcol and latc and lonc:
        # PLUTO stores bbl like "4087860042.00000000"; try the formats until one matches
        forms = [lambda b: b, lambda b: f"'{b}.00000000'", lambda b: f"'{b}'"]
        form = None
        for i in range(0, len(need), 200):
            part = need[i:i + 200]
            for fmt in ([form] if form else forms):
                try:
                    rows = get("/resource/64uk-42ks.json", {"$where": f"{bblcol} in ({','.join(fmt(b) for b in part)})",
                               "$limit": 5000, "$select": f"{bblcol},{latc},{lonc},address"}, tries=2)
                except RuntimeError:
                    rows = []
                if rows:
                    form = fmt
                    break
            for p in rows:
                coords[clean_bbl(p.get(bblcol))] = (fnum(p.get(latc)), fnum(p.get(lonc)), p.get("address"))
            if i % 4000 == 0:
                print(f"   PLUTO coordinates: {len(coords)}/{len(need)}")
    # v0.1: condo units have their own lot numbers that PLUTO doesn't list, so some sales are skipped
    out, used = [], set()
    for l in legals:
        d = docs.get(l["document_id"])
        b = bbl_of(l.get("borough"), l.get("block"), l.get("lot"))
        c = coords.get(b)
        if not d or not c or (l["document_id"], b) in used:
            continue
        used.add((l["document_id"], b))
        amt = fnum(d.get("document_amt"))
        is_deed = d["doc_type"] == "DEED"
        label = "Sold (deed recorded)" if is_deed else "Mortgage recorded"
        addr = f"{l.get('street_number', '')} {l.get('street_name', '')}".strip() or c[2]
        unit = f", unit {l['unit']}" if l.get("unit") else ""
        out.append(ev("property", "Deed" if is_deed else "Mortgage", d.get("recorded_datetime", ""), c[0], c[1],
                      label + (f" - ${amt:,.0f}" if amt else ""), f"{addr}{unit}",
                      "NYC ACRIS", l["document_id"], b, addr, amt))
    print(f"   documents {len(master)}, lots {len(legals)}, unique lots {len(need)}")
    print(f"   located {len(out)} property events ({len(coords)} of {len(need)} lots had coordinates)")
    return [e for e in out if e]


class _Tee:
    def __init__(self, *s): self.s = s
    def write(self, x):
        for f in self.s: f.write(x)
    def flush(self):
        for f in self.s: f.flush()


HISTORY = os.path.join(HERE, "history", "nyc", "first_seen.json")


def apply_first_seen(feats):
    """Remember the first day we saw each record; new restaurants get that day as their date."""
    try:
        with open(HISTORY, encoding="utf-8") as f:
            seen = json.load(f)
    except (OSError, ValueError):
        seen = {}
    today = date.today().isoformat()
    new = 0
    for ft in feats:
        p = ft["properties"]
        key = f"{p['kind']}|{p['rid']}|{'' if p['kind'] == 'business' else p['date']}"
        if key not in seen:
            seen[key] = today
            new += 1
        p["seen"] = seen[key]
        if p["kind"] == "business":
            p["date"] = seen[key]
    cutoff = (date.today() - timedelta(days=400)).isoformat()
    seen = {k: v for k, v in seen.items() if v >= cutoff}
    os.makedirs(os.path.dirname(HISTORY), exist_ok=True)
    with open(HISTORY, "w", encoding="utf-8") as f:
        json.dump(seen, f, separators=(",", ":"), sort_keys=True)
    print(f"   history: {new} records seen for the first time today ({len(seen)} remembered)")


def slim(ft):
    p = ft["properties"]
    p.pop("country", None); p.pop("city", None)
    if p["kind"] == "maintenance":
        p["detail"] = (p.get("detail") or "")[:90]
    p = {k: v for k, v in p.items() if v not in (None, "")}
    ft["properties"] = p
    return ft


def main():
    import sys
    site = "--site" in sys.argv
    sys.stdout = _Tee(sys.__stdout__, open(os.path.join(HERE, "pull_log.txt"), "w", encoding="utf-8"))
    feats, counts, errors = [], {}, {}
    for name, fn in (("construction", permits), ("restaurants", restaurants), ("property", acris)):
        try:
            got = fn()
            feats += got
            counts[name] = len(got)
        except Exception as e:
            errors[name] = str(e)[:400]
            print(f"   FAILED: {e}")
    if not feats:
        raise SystemExit("No data downloaded; keeping the previous files.")
    apply_first_seen(feats)
    feats = [slim(f) for f in feats]
    feats.sort(key=lambda f: f["properties"]["date"])
    # one small file per event type, so the website can load them in parallel
    out_dir = os.path.join(OUT, "nyc")
    os.makedirs(out_dir, exist_ok=True)
    by_kind = {}
    for f in feats:
        by_kind.setdefault(f["properties"]["kind"], []).append(f)
    files = []
    for kind, lst in sorted(by_kind.items()):
        with open(os.path.join(out_dir, f"{kind}.json"), "w", encoding="utf-8") as fh:
            json.dump({"type": "FeatureCollection", "features": lst}, fh, separators=(",", ":"))
        files.append({"kind": kind, "file": f"{kind}.json", "count": len(lst)})
    meta = {"city": "New York", "country": "US", "generated": time.strftime("%Y-%m-%d %H:%M"),
            "since": SINCE, "counts": counts, "errors": errors, "files": files}
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=1)
    if not site:
        # events.js lets index.html load the data by double-click (no web server needed)
        with open(os.path.join(OUT, "events.js"), "w", encoding="utf-8") as f:
            f.write("window.EVENTS=")
            json.dump({"type": "FeatureCollection", "features": feats}, f, separators=(",", ":"))
            f.write(";")
    print(f"\nDone. {len(feats)} events -> data/nyc/")
    print(json.dumps({k: v for k, v in meta.items() if k != "files"}, indent=1))
    if not site:
        print("Now double-click index.html (or refresh it) to see them on the map.")


if __name__ == "__main__":
    main()

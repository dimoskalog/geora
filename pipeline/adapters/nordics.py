"""
Denmark, Finland and Estonia.

Denmark: Fødevarestyrelsen "smiley" food-control results (every food business, latest controls, with coordinates).
Finland: PRH / YTJ open data API v3, companies registered in the last 30 days (street address -> postcode area
         centre from Statistics Finland's open postcode areas).
Estonia: e-Business Register open data (basic data CSV), companies first entered in the last 30 days, geocoded
         with the Estonian Land Board In-ADS gazetteer. Shared "virtual office" addresses are left out.
"""
import csv, io, re, time, zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pipeline.core import http, event, num, title_case, since, log, clean

DAYS = 90


# ---------------------------------------------------------------- Denmark ----
DK_BBOX = [7.8, 54.5, 15.3, 57.8]
DK_URLS = ["https://pub.fvst.dk/publikationer/Smileydata.xml",
           "https://www.findsmiley.dk/xml/allekontrolresultater.xml",
           "https://www.foedevarestyrelsen.dk/_layouts/15/sdata/smiley_xml.zip",
           "https://www.findsmiley.dk/xml/allekontrolresultater.zip"]
SMILEY = {"1": "no remarks", "2": "order issued", "3": "injunction or ban", "4": "fine or police report"}


def _dk_rows(body):
    if body[:2] == b"PK":
        z = zipfile.ZipFile(io.BytesIO(body))
        body = z.read([n for n in z.namelist() if n.lower().endswith(".xml")][0])
    root = ET.fromstring(body)
    for row in root.iter():
        kids = list(row)
        if len(kids) > 8 and not len(kids[0]):
            yield {c.tag.split("}")[-1].lower(): (c.text or "").strip() for c in kids}


def _iso(d):
    d = str(d or "").strip()
    m = re.match(r"(\d{4})-(\d\d)-(\d\d)", d)
    if m:
        return m.group(0)
    m = re.match(r"(\d\d)[.\-/](\d\d)[.\-/](\d{4})", d)
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""


DAWA = "https://api.dataforsyningen.dk"


def dk_address(q, deadline):
    if time.time() > deadline:
        return None, None
    try:
        r = http(f"{DAWA}/adresser", {"q": q, "struktur": "mini", "per_side": 1}, tries=1, timeout=10)
        if r:
            return num(r[0].get("y")), num(r[0].get("x"))
    except RuntimeError:
        pass
    return None, None


def dk_postcode(nr):
    try:
        r = http(f"{DAWA}/postnumre/{nr}", tries=2, timeout=15)
        c = r.get("visueltcenter") or []
        return (num(c[1]), num(c[0])) if len(c) == 2 else (None, None)
    except RuntimeError:
        return None, None


def dk_smiley():
    log("\n== Denmark smiley food controls")
    body = None
    for u in DK_URLS:
        try:
            body = http(u, raw=True, tries=2, timeout=300, headers={"Accept": "*/*"})
            log(f"   {u}: {len(body)} bytes")
            break
        except RuntimeError as e:
            log(f"   {u}: {str(e)[:160]}")
    if not body:
        return []
    recs, n, sample = [], 0, None
    for r in _dk_rows(body):
        n += 1
        sample = sample or r.get("seneste_kontrol_dato")
        for k in ("seneste", "næstseneste", "tredjeseneste", "fjerdeseneste"):
            d = _iso(r.get(f"{k}_kontrol_dato"))
            if d and d >= since(DAYS):
                recs.append((r, d, str(r.get(f"{k}_kontrol_resultat") or r.get(f"{k}_kontrol") or "").strip()))
    log(f"   {n} businesses, {len(recs)} controls in the last {DAYS} days (date sample: {sample!r})")
    if not recs:
        return []
    key = lambda r: (r.get("adresse") or "", r.get("postnummer") or "")
    addrs = sorted({key(r) for r, _, _ in recs if r.get("adresse")})
    deadline = time.time() + 8 * 60              # exact addresses for up to 8 minutes, then postcode centres
    with ThreadPoolExecutor(16) as ex:
        exact = dict(zip(addrs, ex.map(lambda a: dk_address(f"{a[0]}, {a[1]}", deadline), addrs)))
    pcs = sorted({r.get("postnummer") for r, _, _ in recs if r.get("postnummer")})
    with ThreadPoolExecutor(8) as ex:
        cent = dict(zip(pcs, ex.map(dk_postcode, pcs)))
    log(f"   exact addresses {sum(1 for v in exact.values() if v[0])} of {len(addrs)}; postcodes {sum(1 for v in cent.values() if v[0])} of {len(pcs)}")
    out = []
    for r, d, s in recs:
        lat, lon = exact.get(key(r), (None, None))
        if lat is None:
            lat, lon = cent.get(r.get("postnummer"), (None, None))
        name = title_case(r.get("virksomhed") or r.get("navn1") or "")
        addr = f"{r.get('adresse') or ''}, {r.get('postnummer') or ''} {r.get('by') or ''}"
        out.append(event("distress" if s in ("3", "4") else "restaurant", "Food control", d, lat, lon,
                         f"{name}: food control - {SMILEY.get(s, 'smiley ' + s)}", r.get("fvst_branche") or r.get("branche"),
                         "Fødevarestyrelsen smiley", f"{r.get('id_nummer') or r.get('navnelbnr')}-{d}",
                         f"DK {r.get('p_nummer') or r.get('id_nummer')}", addr, None, DK_BBOX))
    log(f"   located {len([x for x in out if x])}")
    return out


# ---------------------------------------------------------------- Finland ----
FI_BBOX = [19.0, 59.5, 31.7, 70.2]
PRH = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"


def fi_postcodes():
    res, err = None, None
    for layer in ("postialue:pno_2026", "postialue:pno_2025", "postialue:pno"):
        try:
            res = http("https://geo.stat.fi/geoserver/postialue/wfs",
                       {"service": "WFS", "version": "2.0.0", "request": "GetFeature", "typeNames": layer,
                        "outputFormat": "application/json", "srsName": "EPSG:4326"}, timeout=600, tries=2)
            log(f"   postcode layer {layer}: {len(res.get('features', []))} areas")
            break
        except RuntimeError as e:
            err = e
            log(f"   postcode layer {layer}: {str(e)[:300]}")
    if not res:
        raise err
    if res.get("features"):
        log("   postcode properties: " + ", ".join(list(res["features"][0].get("properties", {}))[:12]))
    out = {}
    for f in res.get("features", []):
        pts = []
        def walk(c):
            if isinstance(c[0], (int, float)):
                pts.append(c)
            else:
                for x in c:
                    walk(x)
        walk(f["geometry"]["coordinates"])
        if pts:
            a, b = sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)
            lon, lat = (a, b) if a < 40 else (b, a)
            pr = f.get("properties") or {}
            code = pr.get("postinumeroalue") or pr.get("posti_alue") or pr.get("pno")
            out[str(code)] = (lat, lon, pr.get("nimi"))
    return out


def fi_new_companies():
    log("\n== Finland: companies registered in the last 30 days (PRH)")
    comps, page = [], 1
    while page <= 200:
        res = http(PRH, {"registrationDateStart": since(30), "registrationDateEnd": since(0), "page": page}, timeout=120)
        got = res.get("companies") or []
        comps += got
        if page == 1:
            log(f"   totalResults: {res.get('totalResults')}, page size {len(got)}")
        if not got or len(comps) >= (res.get("totalResults") or 0):
            break
        page += 1
    pc = fi_postcodes()
    log(f"   companies: {len(comps)}; postcode areas: {len(pc)}")
    out = []
    for c in comps:
        adds = sorted(c.get("addresses") or [], key=lambda a: a.get("type") != 1)
        a = next((a for a in adds if a.get("postCode") and not a.get("postOfficeBox")), None)
        if not a or a.get("postCode") not in pc:
            continue
        lat, lon, area = pc[a["postCode"]]
        street = f"{a.get('street') or ''} {a.get('buildingNumber') or ''}".strip()
        city = next((p.get("city") for p in a.get("postOffices") or [] if p.get("city")), area) or ""
        name = next((n.get("name") for n in c.get("names") or [] if n.get("name")), "")
        mbl = c.get("mainBusinessLine") or {}
        line = next((d.get("description") for d in mbl.get("descriptions") or [] if d.get("languageCode") == "3"), None) \
            or next((d.get("description") for d in mbl.get("descriptions") or []), "")
        form = next((d.get("description") for f in c.get("companyForms") or [] for d in f.get("descriptions") or []
                     if d.get("languageCode") == "3"), "")
        bid = (c.get("businessId") or {}).get("value")
        out.append(event("business", "Company registered", c.get("registrationDate"), lat, lon,
                         f"{name}: new company", "; ".join(x for x in [line, form, "located at its postcode area centre"] if x),
                         "PRH Trade Register", bid, f"{street}|{a['postCode']}" if street else f"FI {bid}",
                         f"{street}, {a['postCode']} {title_case(city)}".strip(", "), None, FI_BBOX))
    log(f"   located {len([x for x in out if x])}")
    return out


# ---------------------------------------------------------------- Estonia ----
EE_BBOX = [21.5, 57.4, 28.3, 59.9]
EE_CSV = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip"


def ee_geocode(addr):
    try:
        r = http("https://inaadress.maaamet.ee/inaadress/gazetteer", {"address": addr, "results": 1}, tries=2, timeout=20)
        a = (r.get("addresses") or [None])[0]
        if a:
            return num(a.get("viitepunkt_b")), num(a.get("viitepunkt_l"))
    except RuntimeError:
        pass
    return None, None


def ee_new_companies():
    log("\n== Estonia: companies first entered in the last 30 days (e-Business Register)")
    z = zipfile.ZipFile(io.BytesIO(http(EE_CSV, raw=True, timeout=600, headers={"Accept": "*/*"})))
    text = z.read(z.namelist()[0]).decode("utf-8-sig", "replace")
    delim = ";" if text[:2000].count(";") > text[:2000].count(",") else ","
    rows = list(csv.DictReader(io.StringIO(text), delimiter=delim))
    cols = list(rows[0]) if rows else []
    log(f"   rows: {len(rows)}; columns: {cols}")
    col = lambda *keys: next((c for c in cols if all(k in c.lower() for k in keys)), None)
    c_date, c_name, c_code = col("esmakande"), col("nimi"), col("registri", "kood")
    c_addr = col("normaliseeritud") or col("aadress")
    c_status = col("staatus")
    cutoff = since(30)
    new = []
    for r in rows:
        d = str(r.get(c_date) or "")
        if re.match(r"\d\d\.\d\d\.\d{4}", d):
            d = f"{d[6:10]}-{d[3:5]}-{d[0:2]}"
        if d >= cutoff and r.get(c_addr) and "kustut" not in str(r.get(c_status) or "").lower():
            new.append((r, d))
    shared = {}
    for r, _ in new:
        shared[r[c_addr]] = shared.get(r[c_addr], 0) + 1
    keep = [(r, d) for r, d in new if shared[r[c_addr]] < 3]
    log(f"   new: {len(new)}; after leaving out shared virtual-office addresses: {len(keep)}")
    addrs = sorted({r[c_addr] for r, _ in keep})
    deadline = time.time() + 10 * 60
    with ThreadPoolExecutor(8) as ex:
        geo = dict(zip(addrs, ex.map(lambda a: ee_geocode(a) if time.time() < deadline else (None, None), addrs)))
    out = []
    for r, d in keep:
        lat, lon = geo.get(r[c_addr], (None, None))
        out.append(event("business", "Company registered", d, lat, lon, f"{r.get(c_name)}: new company",
                         r.get(col("vorm")) or "", "Estonian e-Business Register", r.get(c_code),
                         r[c_addr], r[c_addr], None, EE_BBOX))
    log(f"   located {len([x for x in out if x])}")
    return out


def regions():
    return [
        ("dk", {"name": "Denmark", "country": "DK", "center": [10.4, 56.0], "focus": [12.5683, 55.6761],
                "focus_name": "Copenhagen", "bbox": DK_BBOX, "sources": ["Fødevarestyrelsen smiley"]}, [dk_smiley]),
        ("fi", {"name": "Finland", "country": "FI", "center": [25.7, 62.5], "focus": [24.9384, 60.1699],
                "focus_name": "Helsinki", "bbox": FI_BBOX, "sources": ["PRH Trade Register", "Statistics Finland postcode areas"]},
         [fi_new_companies]),
        ("ee", {"name": "Estonia", "country": "EE", "center": [25.0, 58.7], "focus": [24.7536, 59.437],
                "focus_name": "Tallinn", "bbox": EE_BBOX, "sources": ["Estonian e-Business Register", "In-ADS gazetteer"]},
         [ee_new_companies]),
    ]

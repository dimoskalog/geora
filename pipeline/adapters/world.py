"""
More keyless sources.

Vancouver   City of Vancouver issued building permits (Open Government Licence - Vancouver), with coordinates.
Winnipeg    City of Winnipeg detailed building permits (Socrata), with coordinates.
Calgary     City of Calgary business licences: first issue date = a genuinely new business (adds to ca-calgary).
New York    NYC Department of Consumer and Worker Protection premises licences created recently (adds to us-nyc).
Singapore   ACRA entities (data.gov.sg), newly registered; located by postal code with OneMap.
Switzerland SHAB / Swiss Official Gazette of Commerce: new registrations (HR01) and deletions (HR03),
            located with the federal geo.admin.ch address search.
Ireland     National planning applications (all councils), with coordinates.
Montreal    Ville de Montréal building permits, with coordinates.
Netherlands BAG buildings under construction (dated by first sight).
"""
import re, time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pipeline.core import http, socrata, event, classify_permit, num, title_case, since, log, clean, point_of

DAYS = 90


# -------------------------------------------------------------- Vancouver ----
VAN = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets"
VAN_BBOX = [-123.3, 49.15, -122.95, 49.35]


def vancouver_permits():
    log("\n== Vancouver issued building permits")
    out, off = [], 0
    while off < 9900:
        res = http(f"{VAN}/issued-building-permits/records",
                   {"where": f"issuedate >= date'{since(DAYS)}'", "order_by": "issuedate desc", "limit": 100, "offset": off})
        rows = res.get("results") or []
        for r in rows:
            g = r.get("geo_point_2d") or {}
            cost = num(r.get("projectvalue"))
            kind, sub, title = classify_permit(r.get("typeofwork"), r.get("projectdescription"), cost)
            if cost and kind != "maintenance":
                title += f" - ${cost:,.0f}"
            use = ", ".join(r.get("specificusecategory") or [])
            out.append(event(kind, sub, r.get("issuedate"), num(g.get("lat")), num(g.get("lon")), title,
                             (f"{use}. " if use else "") + clean(r.get("projectdescription"), 200),
                             "Vancouver Building Permits", r.get("permitnumber"), r.get("address"),
                             str(r.get("address") or "").replace(", Vancouver, BC", ""), cost, VAN_BBOX))
        off += len(rows)
        if len(rows) < 100:
            break
    log(f"   {off} permits")
    return out


# --------------------------------------------------------------- Winnipeg ----
def winnipeg_permits():
    log("\n== Winnipeg building permits")
    rows = socrata("data.winnipeg.ca", "it4w-cpf4", f"issue_date >= '{since(DAYS)}'", "issue_date DESC", 30000, label="permits")
    out = []
    for r in rows:
        lat, lon = point_of(r)
        kind, sub, title = classify_permit(r.get("permit_type") or r.get("work_type"),
                                           f"{r.get('work_type') or ''} {r.get('sub_type') or ''}", None)
        if (r.get("work_type") or "").lower().startswith("new"):
            kind, sub, title = "construction", "New building", "New building permit"
        units = num(r.get("dwelling_units_created")) or 0
        if units:
            title += f" - {int(units)} new home{'s' if units != 1 else ''}"
        out.append(event(kind, sub, r.get("issue_date"), lat, lon, title,
                         " · ".join(x for x in [r.get("permit_group"), r.get("sub_type"), r.get("work_type"),
                                                r.get("applicant_business_name")] if x),
                         "Winnipeg Building Permits", r.get("permit_number"), r.get("address"), r.get("address"),
                         None, [-97.4, 49.75, -96.95, 50.0]))
    return out


# ---------------------------------------------------------------- Calgary ----
def calgary_new_businesses():
    log("\n== Calgary new business licences")
    rows = socrata("data.calgary.ca", "vdjc-pybd", f"first_iss_dt >= '{since(DAYS)}'", "first_iss_dt DESC", 20000,
                   label="licences")
    out = []
    for r in rows:
        if r.get("homeoccind") == "Y":
            continue                              # home-based businesses: not a storefront, and a private home
        lat, lon = point_of(r)
        name = title_case(r.get("tradename"))
        out.append(event("business", "Business licence", r.get("first_iss_dt"), lat, lon, f"{name}: new business licence",
                         title_case(r.get("licencetypes")) + (f" · {title_case(r.get('comdistnm'))}" if r.get("comdistnm") else ""),
                         "Calgary Business Licences", r.get("getbusid"), r.get("address"), r.get("address"),
                         None, [-114.4, 50.8, -113.8, 51.25]))
    return out


# --------------------------------------------------------------- New York ----
def nyc_new_licences():
    log("\n== NYC new premises licences (DCWP)")
    rows = socrata("data.cityofnewyork.us", "w7w3-xahh",
                   f"license_creation_date >= '{since(DAYS)}' AND license_type = 'Premises'", "license_creation_date DESC",
                   20000, label="licences")
    out = []
    for r in rows:
        bbl = str(r.get("bbl") or "").split(".")[0]
        out.append(event("business", "Business licence", r.get("license_creation_date"), num(r.get("latitude")),
                         num(r.get("longitude")), f"{title_case(r.get('business_name'))}: new {r.get('business_category') or 'business'} licence",
                         f"{r.get('business_category') or ''} · {r.get('license_status') or ''}",
                         "NYC DCWP Licenses", r.get("license_nbr"), bbl if len(bbl) == 10 else None,
                         f"{r.get('address_building') or ''} {r.get('address_street_name') or ''}",
                         None, [-74.30, 40.45, -73.65, 40.95]))
    return out


# -------------------------------------------------------------- Singapore ----
SG_BBOX = [103.58, 1.15, 104.1, 1.48]
SG_RES = "d_3f960c10fed6145404ca7b821f263b87"


def onemap(postal):
    try:
        r = http("https://www.onemap.gov.sg/api/common/elastic/search",
                 {"searchVal": postal, "returnGeom": "Y", "getAddrDetails": "Y", "pageNum": 1}, tries=2, timeout=20)
        hit = next((x for x in r.get("results") or [] if x.get("POSTAL") == postal), None) or (r.get("results") or [None])[0]
        if hit:
            return num(hit.get("LATITUDE")), num(hit.get("LONGITUDE")), hit.get("BUILDING")
    except RuntimeError:
        pass
    return None, None, None


def singapore_new_companies():
    log("\n== Singapore newly registered entities (ACRA)")
    rows, off, cutoff = [], 0, since(30)
    while off < 40000:
        res = None
        for i in range(4):
            try:
                res = http("https://data.gov.sg/api/action/datastore_search",
                           {"resource_id": SG_RES, "limit": 5000, "offset": off, "sort": "uen_issue_date desc"}, timeout=180)
                break
            except RuntimeError as e:
                log(f"   data.gov.sg: {str(e)[:120]}; waiting")
                time.sleep(20 * (i + 1))
        recs = ((res or {}).get("result") or {}).get("records") or []
        rows += recs
        off += len(recs)
        if not recs or str(recs[-1].get("uen_issue_date")) < cutoff:
            break
    rows = [r for r in rows if cutoff <= str(r.get("uen_issue_date")) <= since(0) and r.get("reg_postal_code")
            and r.get("issuance_agency_desc") == "ACRA"]
    shared = {}
    for r in rows:
        shared[r["reg_postal_code"]] = shared.get(r["reg_postal_code"], 0) + 1
    keep = [r for r in rows if shared[r["reg_postal_code"]] < 5]
    log(f"   new: {len(rows)}; after leaving out shared corporate-secretary addresses: {len(keep)}")
    codes = sorted({r["reg_postal_code"] for r in keep})
    deadline = time.time() + 8 * 60
    with ThreadPoolExecutor(6) as ex:
        geo = dict(zip(codes, ex.map(lambda c: onemap(c) if time.time() < deadline else (None, None, None), codes)))
    out = []
    for r in keep:
        lat, lon, bldg = geo.get(r["reg_postal_code"], (None, None, None))
        out.append(event("business", "Company registered", r.get("uen_issue_date"), lat, lon,
                         f"{title_case(r.get('entity_name'))}: new {str(r.get('entity_type_desc') or 'entity').lower()}",
                         r.get("entity_type_desc"), "ACRA (data.gov.sg)", r.get("uen"), f"SG {r['reg_postal_code']}",
                         f"{title_case(r.get('reg_street_name'))}{', ' + title_case(bldg) if bldg and bldg != 'NIL' else ''}, Singapore {r['reg_postal_code']}",
                         None, SG_BBOX))
    log(f"   located {len([x for x in out if x])}")
    return out


# ------------------------------------------------------------ Switzerland ----
SHAB = "https://amtsblattportal.ch/api/v1/publications"
CH_BBOX = [5.9, 45.8, 10.5, 47.85]


def _local(tag):
    return tag.split("}")[-1]


def shab_address(xml):
    """Company seat address from a SHAB publication: the first address block outside <meta>."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None
    for el in root.iter():
        if _local(el.tag) == "meta":
            for c in list(el):
                el.remove(c)
    for el in root.iter():
        kids = {_local(c.tag): (c.text or "").strip() for c in el}
        z = kids.get("swissZipCode") or kids.get("zipCode")
        if z and (kids.get("street") or kids.get("town")):
            return {"street": f"{kids.get('street', '')} {kids.get('houseNumber') or kids.get('streetNumber') or ''}".strip(),
                    "zip": z, "town": kids.get("town", "")}
    return None


def swiss_geocode(a):
    q = f"{a['street']} {a['zip']} {a['town']}".strip()
    try:
        r = http("https://api3.geo.admin.ch/rest/services/api/SearchServer",
                 {"searchText": q, "type": "locations", "origins": "address", "limit": 1}, tries=2, timeout=20)
        res = r.get("results") or []
        if not res:
            r = http("https://api3.geo.admin.ch/rest/services/api/SearchServer",
                     {"searchText": f"{a['zip']} {a['town']}", "type": "locations", "origins": "zipcode,gg25", "limit": 1},
                     tries=2, timeout=20)
            res = r.get("results") or []
        if res:
            at = res[0].get("attrs") or {}
            return num(at.get("lat")), num(at.get("lon"))
    except RuntimeError:
        pass
    return None, None


def shab_list(sub, days):
    out, page = [], 0
    while page < 60:
        res = http(SHAB, {"publicationStates": "PUBLISHED", "subRubrics": sub, "publicationDate.start": since(days),
                          "pageRequest.size": 200, "pageRequest.page": page,
                          "pageRequest.sortOrders": "publicationDate,DESC"}, timeout=120)
        items = res.get("content") or []
        out += items
        if len(items) < 200 or str(items[-1]["meta"].get("publicationDate", ""))[:10] < since(days):
            break
        page += 1
    return [p for p in out if str(p["meta"].get("publicationDate", ""))[:10] >= since(days)]


def swiss_register():
    log("\n== Switzerland SHAB commercial register: new entries and deletions")
    pubs = [("HR01", p) for p in shab_list("HR01", 30)] + [("HR03", p) for p in shab_list("HR03", 30)]
    log(f"   publications: {sum(1 for s, _ in pubs if s == 'HR01')} new, {sum(1 for s, _ in pubs if s == 'HR03')} deletions")

    def detail(item):
        sub, p = item
        try:
            x = http(f"{SHAB}/{p['meta']['id']}/xml", raw=True, tries=2, timeout=60, headers={"Accept": "application/xml"})
            return shab_address(x), x
        except RuntimeError:
            return None, None
    deadline = time.time() + 12 * 60
    with ThreadPoolExecutor(8) as ex:
        details = list(ex.map(lambda it: detail(it) if time.time() < deadline else (None, None), pubs))
    addrs = [d[0] for d in details]
    if pubs and not any(addrs):
        sample = next((d[1] for d in details if d[1]), b"")
        log("   no address found; xml sample: " + sample[:1500].decode("utf-8", "replace").replace("\n", " "))
    keys = sorted({(a["street"], a["zip"], a["town"]) for a in addrs if a})
    with ThreadPoolExecutor(8) as ex:
        geo = dict(zip(keys, ex.map(lambda k: swiss_geocode({"street": k[0], "zip": k[1], "town": k[2]}), keys)))
    out = []
    for (sub, p), a in zip(pubs, addrs):
        if not a:
            continue
        lat, lon = geo.get((a["street"], a["zip"], a["town"]), (None, None))
        m = p["meta"]
        t = (m.get("title") or {}).get("en") or ""
        name = re.sub(r"^(New entries|Deletion)\s+", "", t)
        new = sub == "HR01"
        out.append(event("business" if new else "distress", "Company registered" if new else "Removed from register",
                         str(m.get("publicationDate"))[:10], lat, lon,
                         f"{name}: {'new company' if new else 'company removed from register'}",
                         f"Swiss commercial register ({', '.join(m.get('cantons') or [])}) · {m.get('publicationNumber')}",
                         "SHAB / Swiss commercial register", m.get("publicationNumber"),
                         f"{a['street']}|{a['zip']}", f"{a['street']}, {a['zip']} {a['town']}", None, CH_BBOX))
    log(f"   located {len([x for x in out if x])} of {len(pubs)}")
    return out


# ---------------------------------------------------------------- Ireland ----
IE_FS = "https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer/0/query"
IE_BBOX = [-10.7, 51.3, -5.9, 55.5]


def ireland_planning():
    log("\n== Ireland national planning applications")
    out, off = [], 0
    while off < 40000:
        res = http(IE_FS, {"where": f"ReceivedDate >= DATE '{since(DAYS)}'", "outFields": "*", "outSR": 4326, "f": "geojson",
                           "orderByFields": "ReceivedDate DESC", "resultOffset": off, "resultRecordCount": 2000}, timeout=180)
        feats = res.get("features") or []
        if off == 0 and feats:
            log("   fields: " + ", ".join(list(feats[0].get("properties") or {})[:40]))
        for f in feats:
            p = f.get("properties") or {}
            c = (f.get("geometry") or {}).get("coordinates") or [None, None]
            when = p.get("ReceivedDate")
            if isinstance(when, (int, float)):
                when = time.strftime("%Y-%m-%d", time.gmtime(when / 1000))
            desc = clean(p.get("DevelopmentDescription"), 400)
            kind, sub, title = classify_permit(p.get("ApplicationType"), desc, None)
            if kind == "maintenance" and re.search(r"\b(construct|erect|build)\w*\b.*\b(dwelling|house|apartment|units?|building|warehouse|shop|office)", desc, re.I):
                kind, sub, title = "construction", "New building", "Planning application: new building"
            else:
                title = "Planning application" + (f": {sub.lower()}" if kind != "maintenance" else "")
            units = num(p.get("NumResidentialUnits"))
            if units:
                title += f" - {int(units)} home{'s' if units != 1 else ''}"
            status = p.get("ApplicationStatus") or ""
            out.append(event(kind, sub, when, num(c[1]), num(c[0]), title,
                             f"{p.get('PlanningAuthority') or ''} · {status} · {desc}", "Irish planning applications",
                             f"{p.get('PlanningAuthority')}-{p.get('ApplicationNumber')}", clean(p.get("DevelopmentAddress"), 120),
                             clean(p.get("DevelopmentAddress"), 120), None, IE_BBOX))
        off += len(feats)
        if len(feats) < 2000:
            break
    log(f"   {off} applications")
    return out


# --------------------------------------------------------------- Montreal ----
MTL_RES = "5232a72d-235a-48eb-ae20-bb9d501300ad"


def montreal_permits():
    log("\n== Montreal building permits")
    try:
        res = http("https://donnees.montreal.ca/api/3/action/datastore_search_sql",
                   {"sql": f'SELECT * FROM "{MTL_RES}" WHERE date_emission >= \'{since(DAYS)}\' ORDER BY date_emission DESC LIMIT 32000'},
                   timeout=300)
        rows = res["result"]["records"]
    except (RuntimeError, KeyError) as e:
        log(f"   sql not available ({str(e)[:100]}); paging newest first")
        rows, off = [], 0
        while off < 40000:
            res = http("https://donnees.montreal.ca/api/3/action/datastore_search",
                       {"resource_id": MTL_RES, "sort": "date_emission desc", "limit": 5000, "offset": off}, timeout=300)
            recs = res["result"]["records"]
            rows += recs
            off += len(recs)
            if not recs or str(recs[-1].get("date_emission")) < since(DAYS):
                break
    rows = [r for r in rows if str(r.get("date_emission")) >= since(DAYS)]
    log(f"   {len(rows)} permits")
    out = []
    for r in rows:
        units = num(r.get("nb_logements")) or 0
        typ = r.get("description_type_demande") or ""
        kind, sub, title = classify_permit(typ, r.get("nature_travaux"), None)
        if "construction" == typ.lower().strip() or (units and "transformation" not in typ.lower()):
            kind, sub, title = "construction", "New building", "New building permit"
        if units:
            title += f" - {int(units)} home{'s' if units != 1 else ''}"
        out.append(event(kind, sub, r.get("date_emission"), num(r.get("latitude")), num(r.get("longitude")), title,
                         f"{typ} · {r.get('description_type_batiment') or ''} · {clean(r.get('nature_travaux'), 200)}",
                         "Montréal building permits", r.get("id_permis"), r.get("emplacement"),
                         f"{r.get('emplacement') or ''}, {r.get('arrondissement') or ''}", None, [-74.05, 45.38, -73.45, 45.75]))
    return out


# ------------------------------------------------------------ Netherlands ----
BAG_WFS = "https://service.pdok.nl/lv/bag/wfs/v2_0"
NL_BBOX = [3.2, 50.7, 7.3, 53.6]


def dutch_construction():
    """BAG buildings whose status is 'Bouw gestart' (construction started). The register publishes the status, not the
    day it changed, so each building is dated by the first day GEORA saw it under construction."""
    log("\n== Netherlands: buildings under construction (BAG)")
    flt = ("<Filter><PropertyIsEqualTo><PropertyName>status</PropertyName><Literal>Bouw gestart</Literal>"
           "</PropertyIsEqualTo></Filter>")
    out, start = [], 0
    while start < 80000:
        res = http(BAG_WFS, {"service": "WFS", "version": "2.0.0", "request": "GetFeature", "typeName": "bag:pand",
                             "outputFormat": "application/json", "srsName": "EPSG:4326", "count": 1000,
                             "startIndex": start, "filter": flt}, timeout=180)
        feats = res.get("features") or []
        for f in feats:
            p = f.get("properties") or {}
            pts = []
            def walk(c):
                if c and isinstance(c[0], (int, float)):
                    pts.append(c)
                elif c:
                    for x in c:
                        walk(x)
            walk((f.get("geometry") or {}).get("coordinates"))
            if not pts:
                continue
            a_, b_ = sum(q[0] for q in pts) / len(pts), sum(q[1] for q in pts) / len(pts)
            lat, lon = (a_, b_) if a_ > 40 else (b_, a_)    # Dutch latitudes are > 50, longitudes < 8
            units = p.get("aantal_verblijfsobjecten") or 0
            use = str(p.get("gebruiksdoel") or "").replace("functie", "")
            e = event("construction", "Construction started", since(0), lat, lon,
                      "Construction under way" + (f" - {units} unit{'s' if units != 1 else ''}" if units else ""),
                      f"Use: {use}. Planned build year {p.get('bouwjaar') or '?'}. Date shown is the first day GEORA saw construction under way.",
                      "BAG (Kadaster)", p.get("identificatie"), f"BAG {p.get('identificatie')}", None, None, NL_BBOX)
            if e:
                e["properties"]["undated"] = True
            out.append(e)
        start += len(feats)
        if len(feats) < 1000:
            break
    log(f"   {start} buildings under construction")
    return out


def regions():
    return [
        ("ca-vancouver", {"name": "Vancouver", "country": "CA", "center": [-123.12, 49.25], "bbox": VAN_BBOX,
                          "sources": ["Vancouver Building Permits"]}, [vancouver_permits]),
        ("ca-winnipeg", {"name": "Winnipeg", "country": "CA", "center": [-97.14, 49.89], "bbox": [-97.4, 49.75, -96.95, 50.0],
                         "sources": ["Winnipeg Building Permits"]}, [winnipeg_permits]),
        ("ca-calgary", {"sources": ["Calgary Business Licences"]}, [calgary_new_businesses]),
        ("us-nyc", {"sources": ["NYC DCWP Licenses"]}, [nyc_new_licences]),
        ("sg", {"name": "Singapore", "country": "SG", "center": [103.82, 1.35], "bbox": SG_BBOX,
                "sources": ["ACRA (data.gov.sg)", "OneMap"]}, [singapore_new_companies]),
        ("ch", {"name": "Switzerland", "country": "CH", "center": [8.23, 46.8], "focus": [8.5417, 47.3769], "focus_name": "Zurich",
                "bbox": CH_BBOX, "sources": ["SHAB / Swiss commercial register", "geo.admin.ch"]}, [swiss_register]),
        ("ie", {"name": "Ireland", "country": "IE", "center": [-8.0, 53.4], "focus": [-6.2603, 53.3498], "focus_name": "Dublin",
                "bbox": IE_BBOX, "sources": ["Irish planning applications"]}, [ireland_planning]),
        ("ca-montreal", {"name": "Montréal", "country": "CA", "center": [-73.65, 45.53], "bbox": [-74.05, 45.38, -73.45, 45.75],
                         "sources": ["Montréal building permits"]}, [montreal_permits]),
        ("nl", {"name": "Netherlands", "country": "NL", "center": [5.3, 52.2], "focus": [4.9041, 52.3676], "focus_name": "Amsterdam",
                "bbox": NL_BBOX, "sources": ["BAG (Kadaster)"]}, [dutch_construction]),
    ]

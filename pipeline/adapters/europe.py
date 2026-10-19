"""
United Kingdom (all 12 FSA regions) and Norway.

UK:     Food Standards Agency hygiene ratings API (Open Government Licence). Every food business in
        every local authority, one GEORA region per FSA region: recent inspections, and businesses awaiting their first inspection
        (= newly registered food businesses; dated by the first day GEORA saw them).
Norway: Brønnøysund Register Centre, Enhetsregisteret (NLOD 2.0). Every new sub-unit (physical
        business location) registered in the last 30 days, geocoded with Kartverket's open address API.
"""
import threading, time
from concurrent.futures import ThreadPoolExecutor
from pipeline.core import http, event, num, title_case, since, log, clean

DAYS = 90
FSA = "https://api.ratings.food.gov.uk"
FSA_H = {"x-api-version": "2", "Accept": "application/json"}


UK_BBOX = [-8.7, 49.8, 1.8, 60.9]
UK_REGIONS = {  # FSA region name -> (region id, display name, focus city, focus coordinates)
    "London": ("gb-london", "London", None, [-0.1276, 51.5072]),
    "South East": ("gb-se", "South East England", "Brighton", [-0.1372, 50.8225]),
    "East Counties": ("gb-east", "East of England", "Cambridge", [0.1218, 52.2053]),
    "East Midlands": ("gb-em", "East Midlands", "Nottingham", [-1.1581, 52.9548]),
    "West Midlands": ("gb-wm", "West Midlands", "Birmingham", [-1.8904, 52.4862]),
    "North West": ("gb-nw", "North West England", "Manchester", [-2.2426, 53.4808]),
    "North East": ("gb-ne", "North East England", "Newcastle", [-1.6178, 54.9783]),
    "Yorkshire and Humberside": ("gb-yh", "Yorkshire", "Leeds", [-1.5491, 53.8008]),
    "South West": ("gb-sw", "South West England", "Bristol", [-2.5879, 51.4545]),
    "Wales": ("gb-wales", "Wales", "Cardiff", [-3.1791, 51.4816]),
    "Scotland": ("gb-scotland", "Scotland", "Edinburgh", [-3.1883, 55.9533]),
    "Northern Ireland": ("gb-ni", "Northern Ireland", "Belfast", [-5.9301, 54.5973]),
}
_AUTHS = []
_LOCK = threading.Lock()


def fsa_authority(a):
    out, new, page = [], 0, 1
    while True:
        res = http(f"{FSA}/Establishments", {"localAuthorityId": a["LocalAuthorityId"], "pageSize": 5000, "pageNumber": page},
                   headers=FSA_H, timeout=300)
        ests = res.get("establishments", [])
        for e in ests:
            g = e.get("geocode") or {}
            lat, lon = num(g.get("latitude")), num(g.get("longitude"))
            name = clean(e.get("BusinessName"), 80)
            addr = ", ".join(x for x in [e.get("AddressLine1"), e.get("AddressLine2"), e.get("AddressLine3"), e.get("PostCode")] if x)
            rating = str(e.get("RatingValue") or "")
            loc = f"FHRS {e.get('FHRSID')}"
            if rating.replace(" ", "").lower() == "awaitinginspection":
                ev = event("business", "New food business", since(0), lat, lon, f"{name}: new food business, awaiting first inspection",
                           f"{e.get('BusinessType') or ''}. Date shown is the first day GEORA saw it registered.",
                           "UK FSA Food Hygiene Ratings", e.get("FHRSID"), loc, addr, None, UK_BBOX)
                if ev:
                    ev["properties"]["undated"] = True
                    new += 1
                    out.append(ev)
                continue
            rd = str(e.get("RatingDate") or "")[:10]
            if rd >= since(DAYS):
                bad = rating == "0" or rating.lower() == "improvement required"
                note = (" (urgent improvement necessary)" if rating == "0" else " (major improvement necessary)" if rating == "1" else "")
                out.append(event("distress" if bad else "restaurant", "Hygiene rating", rd, lat, lon,
                                 f"{name}: hygiene rating {rating}{note}", e.get("BusinessType"), "UK FSA Food Hygiene Ratings",
                                 f"{e.get('FHRSID')}-{rd}", loc, addr, None, UK_BBOX))
        if len(ests) < 5000:
            break
        page += 1
    return [x for x in out if x], new


def fsa_region(region_name, meta):
    log(f"\n== UK FSA food hygiene: {region_name}")
    with _LOCK:
        if not _AUTHS:
            _AUTHS.extend(http(f"{FSA}/Authorities", headers=FSA_H, timeout=120).get("authorities", []))
    auths = [a for a in _AUTHS if (a.get("RegionName") or "") == region_name]
    out, new = [], 0
    with ThreadPoolExecutor(4) as ex:
        for evs, n in ex.map(fsa_authority, auths):
            out += evs
            new += n
    if out:                                    # fit the region box to the data (1st-99th percentile)
        xs = sorted(f["geometry"]["coordinates"][0] for f in out)
        ys = sorted(f["geometry"]["coordinates"][1] for f in out)
        q = lambda a, f: a[int(f * (len(a) - 1))]
        meta["bbox"] = [round(q(xs, .01) - .1, 3), round(q(ys, .01) - .1, 3), round(q(xs, .99) + .1, 3), round(q(ys, .99) + .1, 3)]
        meta["center"] = [round((meta["bbox"][0] + meta["bbox"][2]) / 2, 3), round((meta["bbox"][1] + meta["bbox"][3]) / 2, 3)]
    log(f"   {region_name}: {len(auths)} authorities, {len(out)} events ({new} new businesses awaiting inspection)")
    return out


# ------------------------------------------------------------------ Norway ----
BRREG = "https://data.brreg.no/enhetsregisteret/api/underenheter"
NO_BBOX = [4.0, 57.8, 31.5, 71.5]


def kartverket(addr, postnr, deadline):
    if time.time() > deadline:
        return None, None
    try:
        r = http("https://ws.geonorge.no/adresser/v1/sok", {"sok": addr, "postnummer": postnr, "treffPerSide": 1}, tries=1, timeout=8)
        a = (r.get("adresser") or [None])[0]
        if a and a.get("representasjonspunkt"):
            p = a["representasjonspunkt"]
            return num(p.get("lat")), num(p.get("lon"))
    except RuntimeError:
        pass
    return None, None


def postcode_centroid(postnr):
    try:
        r = http("https://ws.geonorge.no/adresser/v1/sok", {"postnummer": postnr, "treffPerSide": 1}, tries=1, timeout=8)
        a = (r.get("adresser") or [None])[0]
        if a and a.get("representasjonspunkt"):
            return num(a["representasjonspunkt"].get("lat")), num(a["representasjonspunkt"].get("lon"))
    except RuntimeError:
        pass
    return None, None


def brreg_new_units():
    log("\n== Norway: new business locations (Enhetsregisteret sub-units)")
    units, page = [], 0
    while len(units) < 10000:
        res = http(BRREG, {"fraRegistreringsdatoEnhetsregisteret": since(30), "size": 1000, "page": page}, timeout=120)
        got = (res.get("_embedded") or {}).get("underenheter", [])
        units += got
        if len(got) < 1000 or page >= 9:
            break
        page += 1
    log(f"   new sub-units: {len(units)}")
    todo = []
    for u in units:
        b = u.get("beliggenhetsadresse") or {}
        street = " ".join(b.get("adresse") or []).strip()
        if street and b.get("postnummer"):
            todo.append((u, street, b.get("postnummer"), b.get("poststed") or ""))
    deadline = time.time() + 8 * 60              # exact addresses for up to 8 minutes, then postcode centres
    with ThreadPoolExecutor(16) as ex:
        coords = list(ex.map(lambda t: kartverket(t[1], t[2], deadline), todo))
    missing = sorted({t[2] for t, c in zip(todo, coords) if c[0] is None})
    log(f"   exact addresses: {sum(1 for c in coords if c[0] is not None)}; postcode fallback for {len(missing)} postcodes")
    with ThreadPoolExecutor(16) as ex:
        cent = dict(zip(missing, ex.map(postcode_centroid, missing)))
    coords = [c if c[0] is not None else cent.get(t[2], (None, None)) for t, c in zip(todo, coords)]
    out = []
    for (u, street, pnr, sted), (lat, lon) in zip(todo, coords):
        nk = (u.get("naeringskode1") or {}).get("beskrivelse") or ""
        when = u.get("registreringsdatoEnhetsregisteret") or u.get("oppstartsdato")
        out.append(event("business", "Business location registered", when, lat, lon,
                         f"{title_case(u.get('navn'))}: new business location", nk + (f" · started {u.get('oppstartsdato')}" if u.get("oppstartsdato") else ""),
                         "Brønnøysund Enhetsregisteret", u.get("organisasjonsnummer"), f"{street}|{pnr}", f"{street}, {pnr} {title_case(sted)}",
                         None, NO_BBOX))
    log(f"   located {len([x for x in out if x])} of {len(todo)}")
    return out


def regions():
    uk = []
    for fsa_name, (rid, name, city, focus) in UK_REGIONS.items():
        meta = {"name": name, "country": "GB", "center": focus, "focus": focus, "bbox": UK_BBOX,
                "sources": ["UK FSA Food Hygiene Ratings"]}
        if city:
            meta["focus_name"] = city
        fn = (lambda n=fsa_name, m=meta: fsa_region(n, m))
        fn.__name__ = "fsa"
        uk.append((rid, meta, [fn]))
    return uk + [
        ("no", {"name": "Norway", "country": "NO", "center": [10.0, 62.0], "focus": [10.7522, 59.9139], "focus_name": "Oslo", "bbox": NO_BBOX,
                "sources": ["Brønnøysund Enhetsregisteret", "Kartverket addresses"]}, [brreg_new_units]),
    ]

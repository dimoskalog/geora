"""
United Kingdom (London) and Norway.

UK:     Food Standards Agency hygiene ratings API (Open Government Licence). Every food business in
        the 33 London authorities: recent inspections, and businesses awaiting their first inspection
        (= newly registered food businesses; dated by the first day GEORA saw them).
Norway: Brønnøysund Register Centre, Enhetsregisteret (NLOD 2.0). Every new sub-unit (physical
        business location) registered in the last 30 days, geocoded with Kartverket's open address API.
"""
import time
from concurrent.futures import ThreadPoolExecutor
from pipeline.core import http, event, num, title_case, since, log, clean

DAYS = 90
FSA = "https://api.ratings.food.gov.uk"
FSA_H = {"x-api-version": "2", "Accept": "application/json"}
LONDON_BBOX = [-0.56, 51.27, 0.34, 51.70]


def fsa_london():
    log("\n== UK FSA food hygiene: London")
    auths = http(f"{FSA}/Authorities", headers=FSA_H, timeout=120).get("authorities", [])
    london = [a for a in auths if (a.get("RegionName") or "").lower() == "london"]
    log(f"   London authorities: {len(london)}")
    out, new = [], 0
    for a in london:
        page = 1
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
                if rating == "AwaitingInspection":
                    ev = event("business", "New food business", since(0), lat, lon, f"{name}: new food business, awaiting first inspection",
                               f"{e.get('BusinessType') or ''}. Date shown is the first day GEORA saw it registered.",
                               "UK FSA Food Hygiene Ratings", e.get("FHRSID"), loc, addr, None, LONDON_BBOX)
                    if ev:
                        ev["properties"]["undated"] = True
                        new += 1
                    out.append(ev)
                    continue
                rd = str(e.get("RatingDate") or "")[:10]
                if rd >= since(DAYS):
                    low = rating in ("0", "1")
                    out.append(event("distress" if rating == "0" else "restaurant", "Hygiene rating", rd, lat, lon,
                                     f"{name}: hygiene rating {rating}" + (" (urgent improvement necessary)" if rating == "0" else
                                                                         " (major improvement necessary)" if low else ""),
                                     e.get("BusinessType"), "UK FSA Food Hygiene Ratings", f"{e.get('FHRSID')}-{rd}", loc, addr,
                                     None, LONDON_BBOX))
            if len(ests) < 5000:
                break
            page += 1
    log(f"   events: {len([x for x in out if x])} (new businesses awaiting inspection: {new})")
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
    return [
        ("gb-london", {"name": "London", "country": "GB", "center": [-0.1276, 51.5072], "bbox": LONDON_BBOX,
                       "sources": ["UK FSA Food Hygiene Ratings"]}, [fsa_london]),
        ("no", {"name": "Norway", "country": "NO", "center": [10.0, 62.0], "focus": [10.7522, 59.9139], "focus_name": "Oslo", "bbox": NO_BBOX,
                "sources": ["Brønnøysund Enhetsregisteret", "Kartverket addresses"]}, [brreg_new_units]),
    ]

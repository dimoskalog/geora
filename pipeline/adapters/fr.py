"""
France (national): BODACC legal notices, Alim'confiance food inspections, and SIRENE new
establishments in Paris when an INSEE_KEY is available.

Licence: Licence Ouverte 2.0 (Etalab). Records with partial-diffusion status are excluded.
"""
import csv, io, json, math, os, re, uuid
from pipeline.core import http, event, num, title_case, since, log, clean

REGION = "fr"
META = {"name": "France", "country": "FR", "center": [2.35, 46.6], "bbox": [-5.3, 41.2, 9.7, 51.2],
        "focus": [2.3488, 48.8534], "focus_name": "Paris", "sources": ["BODACC", "Alim'confiance", "SIRENE", "Sitadel (SDES)"]}
BODACC = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/json"
ALIM = "https://dgal.opendatasoft.com/api/explore/v2.1/catalog/datasets/export_alimconfiance/exports/json"
DAYS = 30
# family label -> (kind, sub, title, cap)
FAMILIES = {
    "Ventes et cessions": ("sale", "Business sold", "Business sold", 15000),
    "Procédures collectives": ("distress", "Insolvency proceeding", "Insolvency proceeding", 15000),
    "Créations": ("business", "Business created", "New business", 25000),
    "Radiations": ("distress", "Removed from register", "Company removed from register", 10000),
}


# ------------------------------------------------------------- helpers ----
def jload(s):
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s) if s else None
    except ValueError:
        return None


def find_addr(obj):
    """First dict that looks like a French address inside a nested BODACC structure."""
    if isinstance(obj, dict):
        if "codePostal" in obj and ("nomVoie" in obj or "ville" in obj):
            return obj
        for v in obj.values():
            a = find_addr(v)
            if a:
                return a
    elif isinstance(obj, list):
        for v in obj:
            a = find_addr(v)
            if a:
                return a
    return None


def find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], str):
            return obj[key]
        for v in obj.values():
            r = find_key(v, key)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_key(v, key)
            if r:
                return r
    return None


def norm_fr(street, cp):
    s = re.sub(r"[^A-Z0-9 ]", " ", clean(street).upper()
               .replace("É", "E").replace("È", "E").replace("Ê", "E").replace("À", "A").replace("Ç", "C").replace("Ô", "O"))
    s = " ".join(s.split())
    return f"{s}|{cp}" if s and cp else None


def geocode(rows):
    """rows: list of (id, address, postcode). Returns {id: (lat, lon, score)} via the national address API."""
    out = {}
    endpoints = ["https://data.geopf.fr/geocodage/search/csv", "https://api-adresse.data.gouv.fr/search/csv/"]
    for i in range(0, len(rows), 5000):
        part = rows[i:i + 5000]
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["gid", "adresse", "cp"])
        w.writerows(part)
        b = uuid.uuid4().hex
        fields = [("columns", "adresse"), ("postcode", "cp")]
        body = b""
        for k, v in fields:
            body += f"--{b}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode()
        body += (f"--{b}\r\nContent-Disposition: form-data; name=\"data\"; filename=\"a.csv\"\r\n"
                 f"Content-Type: text/csv\r\n\r\n").encode() + buf.getvalue().encode("utf-8") + f"\r\n--{b}--\r\n".encode()
        res = None
        for ep in endpoints:
            try:
                res = http(ep, data=body, headers={"Content-Type": f"multipart/form-data; boundary={b}", "Accept": "text/csv"},
                           raw=True, tries=2, timeout=300)
                break
            except RuntimeError as e:
                log(f"   geocoder {ep}: {str(e)[:160]}")
        if not res:
            continue
        for r in csv.DictReader(io.StringIO(res.decode("utf-8"))):
            lat, lon, sc = num(r.get("latitude")), num(r.get("longitude")), num(r.get("result_score"))
            if lat and lon and (sc or 0) >= 0.45:
                out[r["gid"]] = (lat, lon, sc)
        log(f"   geocoded {len(out)} of {min(i + 5000, len(rows))}")
    return out


# --------------------------------------------------------------- BODACC ----
def bodacc():
    log("\n== France BODACC (sales of businesses, insolvencies, creations, removals)")
    recs = []
    for fam, (kind, sub, title, cap) in FAMILIES.items():
        rows = http(BODACC, {"where": f'familleavis_lib="{fam}" and dateparution>=date\'{since(DAYS)}\'',
                             "order_by": "dateparution desc", "limit": cap,
                             "select": "id,dateparution,familleavis_lib,typeavis_lib,registre,commercant,ville,cp,"
                                       "listeetablissements,acte,jugement,listepersonnes"}, timeout=600)
        log(f"   {fam}: {len(rows)} notices")
        for r in rows:
            etab = jload(r.get("listeetablissements"))
            a = find_addr(etab) or find_addr(jload(r.get("listepersonnes")))
            street = " ".join(str(a.get(k) or "") for k in ("numeroVoie", "typeVoie", "nomVoie")).strip() if a else ""
            cp = (a or {}).get("codePostal") or r.get("cp") or ""
            city = (a or {}).get("ville") or r.get("ville") or ""
            activity = find_key(etab, "activite") or ""
            enseigne = find_key(etab, "enseigne") or ""
            origine = find_key(etab, "origineFonds") or ""
            price = None
            m = re.search(r"prix (?:stipul[ée] )?de ([\d\s.]+(?:,\d+)?) ?(?:euros|EUR|€)", origine, re.I)
            if m:
                price = num(m.group(1).replace(" ", "").replace(".", "").replace(",", "."))
            name = enseigne or r.get("commercant") or ""
            reg = r.get("registre")
            siren = (reg[0] if isinstance(reg, list) and reg else str(reg or "")).replace(" ", "")[:9]
            ttl = f"{title_case(name)}: {title.lower()}" if name else title
            if kind == "sale" and price:
                ttl += f" - €{price:,.0f}"
            det = "; ".join(x for x in [r.get("typeavis_lib"), activity, origine if kind == "sale" else
                                        (find_key(jload(r.get("jugement")), "nature") or "")] if x)
            recs.append({"gid": str(len(recs)), "street": street, "cp": cp, "city": city, "kind": kind, "sub": sub,
                         "date": r.get("dateparution"), "title": ttl, "detail": det, "rid": r.get("id") or siren,
                         "siren": siren, "amount": price})
    geo = geocode([(x["gid"], f"{x['street']} {x['city']}".strip(), x["cp"]) for x in recs if x["cp"]])
    out = []
    for x in recs:
        g = geo.get(x["gid"])
        if not g:
            continue
        loc = norm_fr(x["street"], x["cp"]) or f"SIREN {x['siren']}"
        out.append(event(x["kind"], x["sub"], x["date"], g[0], g[1], x["title"], x["detail"], "BODACC", x["rid"], loc,
                         f"{x['street']}, {x['cp']} {x['city']}".strip(", "), x["amount"], META["bbox"]))
    return out


# ---------------------------------------------------------- Alim'confiance ----
def alimconfiance():
    log("\n== France Alim'confiance (food safety inspections)")
    rows = http(ALIM, {"where": f"date_inspection>=date'{since(60)}'", "limit": 40000,
                       "select": "siret,app_libelle_etablissement,date_inspection,synthese_eval_sanit,"
                                 "app_libelle_activite_etablissement,adresse_2_ua,code_postal,com_name,geores"}, timeout=600)
    log(f"   inspections: {len(rows)}")
    out = []
    for r in rows:
        g = r.get("geores") or {}
        lat, lon = (num(g.get("lat")), num(g.get("lon"))) if isinstance(g, dict) else (None, None)
        res = r.get("synthese_eval_sanit") or "n/a"
        urgent = "urgente" in res.lower()
        name = title_case(r.get("app_libelle_etablissement") or "")
        out.append(event("restaurant", "Inspection", r.get("date_inspection"), lat, lon,
                         f"{name}: {'urgent hygiene correction ordered' if urgent else 'inspected - ' + res.lower()}",
                         r.get("app_libelle_activite_etablissement"), "Alim'confiance", r.get("siret"),
                         norm_fr(r.get("adresse_2_ua"), r.get("code_postal")),
                         f"{r.get('adresse_2_ua') or ''}, {r.get('code_postal') or ''} {r.get('com_name') or ''}", None, META["bbox"]))
    return out


# ---------------------------------------------------------------- SIRENE ----
def lambert93_to_wgs84(x, y):
    n, c, xs, ys, e, lon0 = 0.7256077650532670, 11754255.426096, 700000.0, 12655612.049876, 0.0818191910428158, math.radians(3)
    dx, dy = x - xs, y - ys
    r = math.hypot(dx, dy)
    gamma = math.atan(dx / -dy)
    lon = lon0 + gamma / n
    latiso = -1 / n * math.log(abs(r / c))
    lat = 2 * math.atan(math.exp(latiso)) - math.pi / 2
    for _ in range(12):
        lat = 2 * math.atan(((1 + e * math.sin(lat)) / (1 - e * math.sin(lat))) ** (e / 2) * math.exp(latiso)) - math.pi / 2
    return math.degrees(lat), math.degrees(lon)


def sirene_paris():
    key = os.environ.get("INSEE_KEY", "").strip()
    if not key:
        log("\n== SIRENE skipped (no INSEE_KEY secret)")
        return []
    log("\n== SIRENE new establishments in Paris (storefront filter)")
    q = f"codeCommuneEtablissement:751* AND dateCreationEtablissement:[{since(DAYS)} TO {since(0)}]"
    rows, cursor = [], "*"
    while len(rows) < 30000:
        page = http("https://api.insee.fr/api-sirene/3.11/siret", {"q": q, "nombre": 1000, "curseur": cursor},
                    headers={"X-INSEE-Api-Key-Integration": key})
        rows += page.get("etablissements", [])
        nxt = page.get("header", {}).get("curseurSuivant")
        if not nxt or nxt == cursor:
            break
        cursor = nxt
    log(f"   new establishments: {len(rows)}")
    def addr(r):
        a = r.get("adresseEtablissement", {})
        return " ".join(str(a.get(k) or "") for k in ("numeroVoieEtablissement", "typeVoieEtablissement",
                                                     "libelleVoieEtablissement")).strip(), a.get("codePostalEtablissement")
    shared = {}
    for r in rows:
        shared[addr(r)] = shared.get(addr(r), 0) + 1
    out = []
    for r in rows:
        if r.get("statutDiffusionEtablissement") != "O":
            continue                           # privacy: partial-diffusion records are excluded
        street, cp = addr(r)
        if shared[(street, cp)] >= 3:
            continue                           # mailbox / domiciliation address, not a real premises
        a = r.get("adresseEtablissement", {})
        x, y = num(a.get("coordonneeLambertAbscisseEtablissement")), num(a.get("coordonneeLambertOrdonneeEtablissement"))
        if not x or not y:
            continue
        lat, lon = lambert93_to_wgs84(x, y)
        per = (r.get("periodesEtablissement") or [{}])[0]
        ul = r.get("uniteLegale", {})
        name = per.get("enseigne1Etablissement") or ul.get("denominationUniteLegale") or ""
        naf = r.get("activitePrincipaleNAF25Etablissement") or per.get("activitePrincipaleEtablissement") or ""
        out.append(event("business", "Establishment created", r.get("dateCreationEtablissement"), lat, lon,
                         f"{title_case(name) or 'New establishment'}: new establishment",
                         f"Activity code {naf}", "SIRENE", r.get("siret"), norm_fr(street, cp), f"{street}, {cp} Paris",
                         None, META["bbox"]))
    return out


# --------------------------------------------------------------- Sitadel ----
DIDO = "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1/datafiles"
SITADEL = {  # rid -> label
    "8b35affb-55fc-4c1f-915b-7750f974446a": "logements",
    "f8f0700f-806c-40a7-83b1-f21cf507e7c4": "locaux",
    "1a9a2f0c-56fe-4e69-84a7-fbbda2121f02": "demolir",
}
LOCAUX = {"COM": "shops", "BUR": "offices", "IND": "industry", "ENT": "warehouses", "HEB": "hotels",
          "PUB": "public buildings", "AGR": "farm buildings"}


def sitadel_rows(rid):
    meta = http(f"{DIDO}/{rid}", timeout=120)
    params = {"millesime": meta.get("millesime"), "withColumnName": "true", "withColumnDescription": "false",
              "withColumnUnit": "false", "DATE_REELLE_AUTORISATION": f"gte:{since(150)}"}
    body = http(f"{DIDO}/{rid}/csv", params, raw=True, timeout=900, headers={"Accept": "text/csv"})
    text = body.decode("utf-8-sig", "replace")
    rows = list(csv.DictReader(io.StringIO(text), delimiter=";" if text[:3000].count(";") > text[:3000].count(",") else ","))
    log(f"   {SITADEL[rid]}: {len(rows)} rows (data up to {meta.get('temporal_coverage', {}).get('end')})")
    return rows


def sitadel():
    log("\n== France Sitadel building permits (homes, commercial space, demolitions)")
    recs = []
    for rid, what in SITADEL.items():
        try:
            rows = sitadel_rows(rid)
        except RuntimeError as e:
            log(f"   {what}: {str(e)[:200]}")
            continue
        for r in rows:
            d = str(r.get("DATE_REELLE_AUTORISATION") or "")[:10]
            if d < since(150):
                continue
            if what == "logements":
                n = int(num(r.get("NB_LGT_TOT_CREES")) or 0)
                if n < 2:
                    continue                       # single houses are left out: thousands a week, little signal each
                title, sub = f"Permit: {n} new homes", "New homes"
                amount = None
            elif what == "locaux":
                parts = {k: num(r.get(f"SURF_{k}_CREEE")) or 0 for k in LOCAUX}
                total = sum(parts.values())
                if total < 200:
                    continue
                main = max(parts, key=parts.get)
                title, sub = f"Permit: {total:,.0f} m² of new {LOCAUX[main]}", "Commercial space"
            else:
                title, sub = "Demolition permit", "Demolition"
            street = f"{r.get('ADR_NUM_TER') or ''} {r.get('ADR_LIBVOIE_TER') or ''}".strip()
            cp = r.get("ADR_CODPOST_TER") or ""
            city = r.get("ADR_LOCALITE_TER") or ""
            num_id = r.get("NUM_DAU") or r.get("NUM_PC") or r.get("NUM_PD") or r.get("NUM_PA") or ""
            recs.append({"gid": str(len(recs)), "street": street, "cp": cp, "city": city, "date": d, "title": title,
                         "sub": sub, "rid": num_id, "detail": "; ".join(x for x in [r.get("NATURE_PROJET_DECLAREE") and
                         f"Project type {r.get('NATURE_PROJET_DECLAREE')}", r.get("DENOM_DEM"), f"Department {r.get('DEP_LIBELLE') or ''}"] if x)})
    log(f"   permits kept: {len(recs)}")
    geo = geocode([(x["gid"], f"{x['street']} {x['city']}".strip(), x["cp"]) for x in recs if x["cp"] and x["street"]])
    out = []
    for x in recs:
        g = geo.get(x["gid"])
        if g:
            out.append(event("construction", x["sub"], x["date"], g[0], g[1], x["title"], x["detail"], "Sitadel (SDES)",
                             x["rid"], norm_fr(x["street"], x["cp"]), f"{x['street']}, {x['cp']} {x['city']}", None, META["bbox"]))
    log(f"   located {len(out)}")
    return out


def regions():
    return [(REGION, META, [bodacc, alimconfiance, sirene_paris, sitadel])]

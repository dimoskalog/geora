"""New York City: DOB permits, DOHMH restaurants, ACRIS deeds and mortgages (PLUTO for coordinates)."""
from pipeline.core import socrata, socrata_columns, http, event, classify_permit, num, title_case, since, log

REGION = "us-nyc"
META = {"name": "New York", "country": "US", "center": [-73.985, 40.742], "bbox": [-74.30, 40.45, -73.65, 40.95],
        "sources": ["NYC DOB NOW: Build", "NYC DOHMH inspections", "NYC ACRIS", "NYC PLUTO"]}
D = "data.cityofnewyork.us"
BORO = {"MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3", "QUEENS": "4", "STATEN ISLAND": "5"}


def bbl_of(boro, block, lot):
    b = str(boro or "").strip().upper()
    b = b if b.isdigit() else BORO.get(b, b)
    try:
        return f"{int(b)}{int(block):05d}{int(lot):04d}"
    except (TypeError, ValueError):
        return None


def clean_bbl(x):
    s = str(x or "").split(".")[0]
    return s if len(s) == 10 and s.isdigit() else None


def permits():
    log("\n== NYC DOB NOW permits")
    rows = socrata(D, "rbx6-tga4", f"issued_date >= '{since(90)}'", "issued_date DESC", 80000, label="permits")
    out = []
    for r in rows:
        cost = num(r.get("estimated_job_costs"))
        wt = title_case(r.get("work_type") or "Work")
        desc = r.get("job_description") or ""
        kind, sub, title = classify_permit(wt, desc, cost)
        if cost and kind != "maintenance":
            title += f" - ${cost:,.0f}"
        bbl = clean_bbl(r.get("bbl")) or bbl_of(r.get("borough"), r.get("block"), r.get("lot"))
        out.append(event(kind, sub, r.get("issued_date"), num(r.get("latitude")), num(r.get("longitude")), title,
                         desc, "NYC DOB NOW: Build", r.get("work_permit") or r.get("job_filing_number"), bbl,
                         f"{r.get('house_no', '')} {r.get('street_name', '')}", cost, META["bbox"]))
    return out


def restaurants():
    log("\n== NYC restaurant inspections")
    sel = "camis,dba,building,street,cuisine_description,inspection_date,inspection_type,action,grade,latitude,longitude,bbl"
    rows = socrata(D, "43nn-pn8j", f"inspection_date >= '{since(90)}'", "inspection_date DESC", 60000, sel, "inspection rows")
    new = socrata(D, "43nn-pn8j", "inspection_date < '1901-01-01'", "camis", 20000, sel, "new restaurant rows")
    out, seen = [], set()
    for r in rows:                                   # one row per violation -> one event per visit
        k = (r.get("camis"), r.get("inspection_date"))
        if k in seen:
            continue
        seen.add(k)
        closed = "closed" in (r.get("action") or "").lower()
        g = r.get("grade")
        name = title_case(r.get("dba"))
        title = f"{name}: " + ("Closed by health department" if closed else f"Inspected{' - grade ' + g if g in ('A', 'B', 'C') else ''}")
        out.append(event("distress" if closed else "restaurant", "Closure order" if closed else "Inspection",
                         r.get("inspection_date"), num(r.get("latitude")), num(r.get("longitude")), title,
                         f"{r.get('inspection_type') or 'Inspection'}. {r.get('cuisine_description') or ''}",
                         "NYC DOHMH inspections", r.get("camis"), clean_bbl(r.get("bbl")),
                         f"{r.get('building', '')} {r.get('street', '')}", None, META["bbox"]))
    seen = set()
    for r in new:
        if r.get("camis") in seen:
            continue
        seen.add(r.get("camis"))
        e = event("business", "New restaurant permit", since(0), num(r.get("latitude")), num(r.get("longitude")),
                  f"{title_case(r.get('dba'))}: new restaurant, not yet inspected",
                  f"{r.get('cuisine_description') or ''}. Date shown is the first day this permit appeared in the city's data.",
                  "NYC DOHMH inspections", r.get("camis"), clean_bbl(r.get("bbl")),
                  f"{r.get('building', '')} {r.get('street', '')}", None, META["bbox"])
        if e:
            e["properties"]["undated"] = True
        out.append(e)
    return out


def _pluto(where, select):
    return http(f"https://{D}/resource/64uk-42ks.json", {"$where": where, "$limit": 50000, "$select": select}, tries=2)


def property_records():
    log("\n== NYC ACRIS deeds and mortgages (published with a 2-3 month delay)")
    master = socrata(D, "bnx9-e6tj", f"recorded_datetime >= '{since(180)}' AND doc_type in ('DEED','MTGE')",
                     "recorded_datetime DESC", 30000, "document_id,doc_type,document_amt,recorded_datetime", "documents")
    docs = {m["document_id"]: m for m in master}
    ids, legals = list(docs), []
    for i in range(0, len(ids), 150):
        chunk = ",".join(f"'{x}'" for x in ids[i:i + 150])
        legals += http(f"https://{D}/resource/8h5j-fqxa.json", {"$where": f"document_id in ({chunk})", "$limit": 5000,
                       "$select": "document_id,borough,block,lot,street_number,street_name,unit"})
    need = sorted({b for b in (bbl_of(l.get("borough"), l.get("block"), l.get("lot")) for l in legals) if b})
    coords = {}
    for i in range(0, len(need), 200):              # bbl is numeric in PLUTO
        for p in _pluto(f"bbl in ({','.join(need[i:i + 200])})", "bbl,latitude,longitude,address"):
            coords[clean_bbl(p.get("bbl"))] = (num(p.get("latitude")), num(p.get("longitude")), p.get("address"))
    # condo units (lots 1001+) are not in PLUTO: use any lot on the same block (the condo's billing lot)
    missing = {(b[0], int(b[1:6])) for b in need if b not in coords}
    by_block = {}
    for boro in "12345":
        blocks = sorted({blk for bo, blk in missing if bo == boro})
        for i in range(0, len(blocks), 150):
            q = f"borocode='{boro}' AND block in ({','.join(repr(str(b)) for b in blocks[i:i + 150])})"
            try:
                for p in _pluto(q, "borocode,block,lot,latitude,longitude,address"):
                    k = (boro, int(num(p.get("block")) or 0))
                    lot = int(num(p.get("lot")) or 0)
                    if num(p.get("latitude")) and (k not in by_block or lot >= 7500):
                        by_block[k] = (num(p.get("latitude")), num(p.get("longitude")), p.get("address"))
            except RuntimeError as e:
                log(f"   condo block lookup failed: {e}")
                break
    out, used = [], set()
    for l in legals:
        d = docs.get(l["document_id"])
        b = bbl_of(l.get("borough"), l.get("block"), l.get("lot"))
        c = coords.get(b) or (by_block.get((b[0], int(b[1:6]))) if b else None)
        if not d or not c or (l["document_id"], b) in used:
            continue
        used.add((l["document_id"], b))
        amt = num(d.get("document_amt"))
        deed = d["doc_type"] == "DEED"
        addr = f"{l.get('street_number', '')} {l.get('street_name', '')}".strip() or c[2]
        unit = f", unit {l['unit']}" if l.get("unit") else ""
        out.append(event("sale" if deed else "mortgage", "Deed" if deed else "Mortgage", d.get("recorded_datetime"),
                         c[0], c[1], ("Sold (deed recorded)" if deed else "Mortgage recorded") + (f" - ${amt:,.0f}" if amt else ""),
                         f"{addr}{unit}", "NYC ACRIS", l["document_id"], b, addr, amt, META["bbox"]))
    log(f"   documents {len(master)}, lots {len(need)}, located {len(coords)} + condo blocks {len(by_block)}")
    return out


def regions():
    return [(REGION, META, [permits, restaurants, property_records])]

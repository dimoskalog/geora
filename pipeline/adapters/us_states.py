"""
US statewide sources (cover every city in the state at once). Addresses are geocoded with the
free US Census batch geocoder.

  Texas:    Comptroller sales-tax permits (new outlets), TABC liquor licence applications and issues
  Colorado: Secretary of State business entity registrations
"""
from pipeline.core import socrata, event, num, title_case, since, log, geocode_us
from pipeline.adapters.us_cities import norm_addr, iso_date

DAYS = 30


def located(rows, build, state, bbox, label):
    """rows -> events; build(row) returns (args-without-coords, street, city, zip)."""
    items = []
    for r in rows:
        b = build(r)
        if b:
            items.append(b)
    geo = geocode_us([(str(i), st, ci, state, z) for i, (_, st, ci, z) in enumerate(items)])
    out = []
    for i, (a, *_rest) in enumerate(items):
        g = geo.get(str(i))
        if g:
            kind, sub, when, title, desc, source, rid, addr, amount = a
            out.append(event(kind, sub, when, g[0], g[1], title, desc, source, rid, norm_addr(addr), addr, amount, bbox))
    log(f"   {label}: {len(out)} located of {len(items)}")
    return out


TX_BBOX = [-106.7, 25.8, -93.5, 36.6]


def tx_sales_tax():
    log("\n== Texas new sales-tax permits (new outlets, statewide)")
    rows = socrata("data.texas.gov", "jrea-zgmq", f"outlet_permit_issue_date >= '{since(DAYS)}'",
                   "outlet_permit_issue_date DESC", 30000, label="permits")
    def build(r):
        when = iso_date(r.get("outlet_permit_issue_date"))
        first = iso_date(r.get("outlet_first_sales_date"))
        if not when or not r.get("outlet_address"):
            return None
        name = title_case(r.get("outlet_name") or r.get("taxpayer_name"))
        future = first and first > when
        desc = (f"Declared first-sales date {first}" + (" (in the future: likely opening soon)" if future else "")
                if first else "") + f"; NAICS {r.get('outlet_naics_code') or 'n/a'}"
        args = ("business", "Sales-tax permit", when, f"{name}: new sales-tax permit", desc,
                "Texas Comptroller sales-tax permits", f"{r.get('taxpayer_number')}-{r.get('outlet_number')}",
                f"{r.get('outlet_address')}, {title_case(r.get('outlet_city'))}", None)
        return args, r.get("outlet_address"), r.get("outlet_city"), (r.get("outlet_zip_code") or "")[:5]
    return located(rows, build, "TX", TX_BBOX, "sales-tax permits")


def tx_tabc():
    log("\n== Texas liquor licences (TABC): pending applications and new licences")
    pend = socrata("data.texas.gov", "mxm5-tdpj", f"submission_date >= '{since(90)}'", "submission_date DESC", 10000, label="pending")
    lic = socrata("data.texas.gov", "7hf9-qc9f", f"original_issue_date >= '{since(90)}'", "original_issue_date DESC", 10000, label="issued")
    def b_pend(r):
        when = iso_date(r.get("submission_date"))
        if not when or not r.get("address"):
            return None
        args = ("business", "Liquor licence application", when, f"{title_case(r.get('trade_name') or r.get('owner'))}: liquor licence applied for",
                f"{r.get('license_type') or ''} · {r.get('applicationstatus') or ''}", "TABC pending applications",
                r.get("applicationid"), f"{r.get('address')}, {title_case(r.get('city'))}", None)
        return args, r.get("address"), r.get("city"), (r.get("zip") or "")[:5]
    def b_lic(r):
        when = iso_date(r.get("original_issue_date"))
        if not when or not r.get("address"):
            return None
        args = ("business", "Liquor licence issued", when, f"{title_case(r.get('trade_name') or r.get('owner'))}: liquor licence issued",
                f"{r.get('license_type') or ''} · {r.get('primary_status') or ''}", "TABC licences",
                r.get("license_id"), f"{r.get('address')}, {title_case(r.get('city'))}", None)
        return args, r.get("address"), r.get("city"), (r.get("zip") or "")[:5]
    return located(pend, b_pend, "TX", TX_BBOX, "pending") + located(lic, b_lic, "TX", TX_BBOX, "issued")


CO_BBOX = [-109.1, 36.9, -102.0, 41.1]


def co_entities():
    log("\n== Colorado new business registrations (statewide)")
    rows = socrata("data.colorado.gov", "4ykn-tg5h", f"entityformdate >= '{since(DAYS)}'", "entityformdate DESC", 30000, label="entities")
    def build(r):
        when = iso_date(r.get("entityformdate"))
        if not when or not r.get("principaladdress1") or (r.get("principalstate") or "CO") != "CO":
            return None
        args = ("business", "Business registered", when, f"{title_case(r.get('entityname'))}: new {(r.get('entitytype') or 'business').lower()}",
                "Registered address (may be an agent or home address, not a storefront)", "Colorado Secretary of State",
                r.get("entityid"), f"{r.get('principaladdress1')}, {title_case(r.get('principalcity'))}", None)
        return args, r.get("principaladdress1"), r.get("principalcity"), (r.get("principalzipcode") or "")[:5]
    return located(rows, build, "CO", CO_BBOX, "registrations")


def regions():
    return [
        ("us-tx", {"name": "Texas", "country": "US", "center": [-99.0, 31.0], "focus": [-95.3698, 29.7604], "bbox": TX_BBOX,
                   "sources": ["Texas Comptroller sales-tax permits", "TABC"]}, [tx_sales_tax, tx_tabc]),
        ("us-co", {"name": "Colorado", "country": "US", "center": [-105.5, 39.0], "focus": [-104.9903, 39.7392], "bbox": CO_BBOX,
                   "sources": ["Colorado Secretary of State"]}, [co_entities]),
    ]

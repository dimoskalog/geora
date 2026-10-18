"""US cities on non-Socrata portals: Boston (CKAN) and Philadelphia (Carto SQL)."""
from pipeline.core import http, event, classify_permit, num, title_case, since, point_of, log
from pipeline.adapters.us_cities import norm_addr, iso_date

DAYS = 90


def boston():
    log("\n== Boston approved building permits (CKAN)")
    res = "6ddcd912-32a0-43df-9908-63574f8c7e77"
    sql = f'SELECT * FROM "{res}" WHERE issued_date >= \'{since(DAYS)}\' ORDER BY issued_date DESC LIMIT 40000'
    rows = http("https://data.boston.gov/api/3/action/datastore_search_sql", {"sql": sql}, timeout=300)["result"]["records"]
    log(f"   rows: {len(rows)}")
    out = []
    for r in rows:
        lat, lon = point_of(r, lat_keys=("y_latitude", "gpsy", "lat", "latitude"), lon_keys=("x_longitude", "gpsx", "long", "longitude"))
        cost = num(r.get("declared_valuation") or r.get("total_fees"))
        desc = r.get("comments") or r.get("description") or ""
        kind, sub, title = classify_permit(r.get("permittypedescr") or r.get("worktype"), f"{r.get('description', '')} {desc}", cost)
        if cost and kind != "maintenance":
            title += f" - ${cost:,.0f}"
        addr = r.get("address") or ""
        out.append(event(kind, sub, iso_date(r.get("issued_date")), lat, lon, title, desc, "Boston Approved Building Permits",
                         r.get("permitnumber"), norm_addr(addr), addr, cost, [-71.20, 42.22, -70.98, 42.40]))
    return out


CARTO = "https://phl.carto.com/api/v2/sql"


def philly_permits():
    log("\n== Philadelphia building permits (Carto)")
    q = ("SELECT permitnumber, permitissuedate, permitdescription, typeofwork, approvedscopeofwork, address, "
         "ST_Y(the_geom) AS lat, ST_X(the_geom) AS lng FROM permits "
         f"WHERE permitissuedate >= '{since(DAYS)}' ORDER BY permitissuedate DESC LIMIT 40000")
    rows = http(CARTO, {"q": q}, timeout=300).get("rows", [])
    log(f"   rows: {len(rows)}")
    out = []
    for r in rows:
        desc = r.get("approvedscopeofwork") or ""
        kind, sub, title = classify_permit(f"{r.get('permitdescription') or ''} {r.get('typeofwork') or ''}", desc, None)
        out.append(event(kind, sub, iso_date(r.get("permitissuedate")), num(r.get("lat")), num(r.get("lng")), title, desc,
                         "Philadelphia L&I Permits", r.get("permitnumber"), norm_addr(r.get("address")), r.get("address"),
                         None, [-75.29, 39.86, -74.95, 40.14]))
    return out


def philly_licenses():
    log("\n== Philadelphia new business licenses (Carto)")
    q = ("SELECT licensenum, initialissuedate, business_name, licensetype, address, "
         "ST_Y(the_geom) AS lat, ST_X(the_geom) AS lng FROM business_licenses "
         f"WHERE initialissuedate >= '{since(DAYS)}' ORDER BY initialissuedate DESC LIMIT 30000")
    rows = http(CARTO, {"q": q}, timeout=300).get("rows", [])
    log(f"   rows: {len(rows)}")
    return [event("business", "Business licensed", iso_date(r.get("initialissuedate")), num(r.get("lat")), num(r.get("lng")),
                  f"{title_case(r.get('business_name')) or 'Business'}: new business license", r.get("licensetype"),
                  "Philadelphia Business Licenses", r.get("licensenum"), norm_addr(r.get("address")), r.get("address"),
                  None, [-75.29, 39.86, -74.95, 40.14]) for r in rows]


def regions():
    return [
        ("us-boston", {"name": "Boston", "country": "US", "center": [-71.0589, 42.3601], "bbox": [-71.20, 42.22, -70.98, 42.40],
                       "sources": ["Boston Approved Building Permits"]}, [boston]),
        ("us-philly", {"name": "Philadelphia", "country": "US", "center": [-75.1652, 39.9526], "bbox": [-75.29, 39.86, -74.95, 40.14],
                       "sources": ["Philadelphia L&I Permits", "Philadelphia Business Licenses"]}, [philly_permits, philly_licenses]),
    ]

"""Fetch a sample of each candidate source and write history/probe.txt (schema discovery for new adapters)."""
import json, sys
from concurrent.futures import ThreadPoolExecutor
from pipeline.core import http

URLS = [l.strip() for l in open("pipeline/probe_urls.txt") if l.strip() and not l.startswith("#")]


def get(u):
    method, data, url = "GET", None, u
    if u.startswith("POST "):
        _, url, body = u.split(" ", 2)
        data = body.encode()
    try:
        b = http(url, raw=True, tries=2, timeout=90, data=data,
                 headers={"Accept": "*/*", **({"Content-Type": "application/json"} if data else {})})
        return f"### {u}\n{len(b)} bytes\n{b[:2500].decode('utf-8', 'replace')}\n"
    except Exception as e:
        return f"### {u}\nERROR {str(e)[:400]}\n"


with ThreadPoolExecutor(8) as ex:
    out = list(ex.map(get, URLS))
open("history/probe.txt", "w", encoding="utf-8").write("\n".join(out))
print("\n".join(o[:300] for o in out))

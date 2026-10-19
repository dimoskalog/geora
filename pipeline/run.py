"""
Run every adapter, write data/<region>/*.json, data/regions.json and history/last_run.json.

  python -m pipeline.run                 all regions
  python -m pipeline.run --only fr,us-sf  some regions
"""
import json, os, sys, time, traceback
from concurrent.futures import ThreadPoolExecutor
from pipeline import core
from pipeline.adapters import nyc, us_cities, us_other, us_hubs, us_states, fr, europe, nordics

ADAPTERS = [nyc, us_cities, us_other, us_hubs, us_states, fr, europe, nordics]


def main():
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    started = time.time()
    regions_out, status = [], {"started": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()), "regions": {}}
    jobs = []
    for adapter in ADAPTERS:
        try:
            jobs += [j for j in adapter.regions() if not only or j[0] in only]
        except Exception as e:
            core.log(f"ADAPTER {adapter.__name__} FAILED: {e}")

    def run_region(job):
        rid, meta, fns = job
        core.log(f"######## start {rid} · {meta['name']}")
        feats, parts = [], {}
        for fn in fns:
            name = getattr(fn, "__name__", "source")
            t = time.time()
            try:
                got = [f for f in fn() if f]
                feats += got
                parts[name] = {"events": len(got), "seconds": round(time.time() - t)}
            except Exception as e:
                parts[name] = {"error": str(e)[:500]}
                core.log(f"   [{rid}] FAILED {name}: {e}")
        status["regions"][rid] = {"name": meta["name"], "events": len(feats), "parts": parts}
        if not feats:
            core.log(f"   [{rid}] no events; keeping it off the map")
            return None
        try:
            core.apply_history(rid, feats)
            m = core.write_region(rid, meta, feats)
            core.log(f"######## done {rid}: {len(feats)} events")
            return {k: m[k] for k in ("id", "name", "country", "center", "bbox", "events", "first", "last",
                                      "generated", "sources", "focus", "focus_name") if k in m}
        except Exception:
            core.log(f"   [{rid}] WRITE FAILED:\n" + traceback.format_exc())
            status["regions"][rid]["write_error"] = traceback.format_exc()[-800:]
            return None

    # regions are independent and mostly waiting on the network: run several at once
    with ThreadPoolExecutor(8) as ex:
        results = list(ex.map(run_region, jobs))
    order = {j[0]: i for i, j in enumerate(jobs)}
    regions_out = sorted([r for r in results if r], key=lambda r: order[r["id"]])
    os.makedirs(core.DATA, exist_ok=True)
    if only:                                  # keep regions that were not re-run this time
        try:
            old = json.load(open(os.path.join(core.DATA, "regions.json"), encoding="utf-8"))["regions"]
            regions_out += [r for r in old if r["id"] not in {x["id"] for x in regions_out}]
        except (OSError, ValueError, KeyError):
            pass
    with open(os.path.join(core.DATA, "regions.json"), "w", encoding="utf-8") as f:
        json.dump({"generated": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()), "regions": regions_out}, f, indent=1)
    status["minutes"] = round((time.time() - started) / 60, 1)
    status["log_tail"] = core.LOG[-120:]
    os.makedirs(core.HISTORY, exist_ok=True)
    with open(os.path.join(core.HISTORY, "last_run.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, indent=1, ensure_ascii=False)
    core.log(f"\nDone in {status['minutes']} min: " + ", ".join(f"{r['id']} {r['events']}" for r in regions_out))


if __name__ == "__main__":
    main()

"""
Run every adapter, write data/<region>/*.json, data/regions.json and history/last_run.json.

  python -m pipeline.run                 all regions
  python -m pipeline.run --only fr,us-sf  some regions
"""
import json, os, sys, time, traceback
from pipeline import core
from pipeline.adapters import nyc, us_cities, us_other, us_hubs, us_states, fr, europe

ADAPTERS = [nyc, us_cities, us_other, us_hubs, us_states, fr, europe]


def main():
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    started = time.time()
    regions_out, status = [], {"started": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()), "regions": {}}
    for adapter in ADAPTERS:
        try:
            regs = adapter.regions()
        except Exception as e:
            core.log(f"ADAPTER {adapter.__name__} FAILED: {e}")
            continue
        for rid, meta, fns in regs:
            if only and rid not in only:
                continue
            core.log(f"\n######## {rid} · {meta['name']}")
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
                    core.log(f"   FAILED {name}: {e}")
                    core.log("   " + traceback.format_exc().splitlines()[-1])
            status["regions"][rid] = {"name": meta["name"], "events": len(feats), "parts": parts}
            if not feats:
                core.log(f"   no events for {rid}; keeping it off the map")
                continue
            try:
                core.apply_history(rid, feats)
                m = core.write_region(rid, meta, feats)
                regions_out.append({k: m[k] for k in ("id", "name", "country", "center", "bbox", "events", "first", "last",
                                                      "generated", "sources", "focus", "focus_name") if k in m})
            except Exception:
                core.log(f"   WRITE FAILED for {rid}:\n" + traceback.format_exc())
                status["regions"][rid]["write_error"] = traceback.format_exc()[-800:]
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

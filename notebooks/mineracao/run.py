# Run one pipeline from the command line (same code the notebooks use).

# python3 run.py <cfem|amb|anp|scm|sigbm|dipem|flow> [chave=valor ...] [--dry-run] [--profile] [--cores N]

# Values are parsed as JSON when possible (true, 12, ["a","b"]); anything else stays a string.

# Example: python3 run.py cfem mode=range start_date=2025-01 end_date=2025-03 target_db=estudo

import json
import sys
import time

from pipelines import common as C


def parse(v):
    try:
        return json.loads(v)
    except ValueError:
        return v


def main(argv):
    if not argv or argv[0] not in ("cfem", "amb", "anp", "scm", "sigbm", "dipem", "flow"):
        sys.exit(__doc__)
    name, kw, flags, cores = argv[0], {}, set(), 2
    it = iter(argv[1:])
    for a in it:
        if a == "--cores":
            cores = int(next(it))
        elif a.startswith("--"):
            flags.add(a[2:])
        else:
            k, v = a.split("=", 1)
            kw[k] = parse(v)
    kw["dry_run"] = "dry-run" in flags
    if "profile" in flags and name != "flow":
        kw["profile"] = True
    spark = None if kw["dry_run"] else C.get_spark("run_" + name, cores_max=cores)
    t0 = time.time()
    try:
        if name == "flow":
            from pipelines.flow import run_flow
            res = run_flow(spark, **kw)
        else:
            import importlib
            res = importlib.import_module("pipelines." + name).main(spark, **kw)
    finally:
        if spark is not None:
            spark.stop()
    print("\n=== RESULT (%s, %ds, storage=%s) ===" % (name, round(time.time() - t0), C.STORAGE))
    print(json.dumps(res, ensure_ascii=False, indent=1, default=str)[:6000])


if __name__ == "__main__":
    main(sys.argv[1:])

# Run one pipeline from the command line (same code the notebooks use).

# python3 run.py <cnpj|cvm|pgfn|cgu|mte|pncp|caged|flow> [chave=valor ...] [--dry-run] [--cores N] [--local]

# --local runs Spark inside this container (local[N]) instead of on the cluster.

# Values are parsed as JSON when possible (true, 12, ["a","b"]); anything else stays a string.

# Example: python3 run.py cnpj 'ufs=["ES"]' target_db=estudo

import importlib
import json
import sys
import time

from pipelines import flow  # noqa: F401  (also puts notebooks/ on sys.path for `lakehouse`)
from lakehouse import core as C

PIPELINES = ("cnpj", "cvm", "pgfn", "cgu", "mte", "pncp", "caged", "flow")


def parse(v):
    try:
        return json.loads(v)
    except ValueError:
        return v


def main(argv):
    if not argv or argv[0] not in PIPELINES:
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
    master = "local[%d]" % cores if "local" in flags else "spark://spark-master:7077"
    spark = None if kw["dry_run"] else C.get_spark("run_" + name, cores_max=cores, master=master)
    t0 = time.time()
    try:
        if name == "flow":
            res = flow.run_flow(spark, **kw)
        else:
            res = importlib.import_module("pipelines." + name).main(spark, **kw)
    finally:
        if spark is not None:
            spark.stop()
    print("\n=== RESULT (%s, %ds, storage=%s) ===" % (name, round(time.time() - t0), C.STORAGE))
    print(json.dumps(res, ensure_ascii=False, indent=1, default=str)[:6000])


if __name__ == "__main__":
    main(sys.argv[1:])

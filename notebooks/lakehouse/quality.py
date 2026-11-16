# Data quality checks on what a load is about to make live (card #9).
#
# Each pipeline declares what its tables must satisfy, next to the table name:
#
#     Q.expect("anm_cfem_arrecadacao", pk=["ano_mes", "chave"], not_null=["valor"], ranges={"valor": (0, None)})
#
# commit() (lakehouse/core.py and mineracao/pipelines/common.py) runs the checks on the result that would replace the
# live table (the staged rows for a snapshot, the merged table for an upsert, each rewritten partition), before the
# swap. A failing hard check raises DataQualityError: the stage is dropped and the live table is left as it was, like a
# quota error. Soft checks only print a warning. Every run's report is kept with the load (core: <db>/_ctl/quality/).
#
# Hard checks: not empty (unless allow_empty), primary key unique and not null, not_null columns.
# Soft checks: ranges (min, max; None = open) and allowed values. Pass hard=["ranges", "allowed"] to make them fail.
# A key not yet confirmed on the real data is declared with pk_soft=True: duplicates and nulls in it only warn.
# Every table gets the not-empty check, declared or not. Tables named at run time are declared by prefix (prefix=True).
#
# PySpark native functions only; one aggregation pass for counts and nulls, one group-by for the key.
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

Range = Tuple[Optional[float], Optional[float]]


class DataQualityError(RuntimeError):
    """a hard check failed: nothing was committed"""

    def __init__(self, table: str, failures: List[str], report: dict):
        self.table = table
        self.failures = failures
        self.report = report
        super().__init__("Data quality: %s not committed (live table intact): %s" % (table, "; ".join(failures)))


@dataclass
class Expect:
    pk: Tuple[str, ...] = ()
    not_null: Tuple[str, ...] = ()
    ranges: Dict[str, Range] = field(default_factory=dict)
    allowed: Dict[str, Tuple] = field(default_factory=dict)
    allow_empty: bool = False
    hard: Tuple[str, ...] = ()  # soft check kinds promoted to hard: "ranges", "allowed"
    pk_soft: bool = False  # the declared key isn't confirmed on the real data yet: its duplicates/nulls only warn


_REGISTRY: Dict[str, Expect] = {}
_PREFIXES: Dict[str, Expect] = {}


def expect(table: str, pk: Sequence[str] = (), not_null: Sequence[str] = (), ranges: Optional[Dict[str, Range]] = None,
           allowed: Optional[Dict[str, Iterable]] = None, allow_empty: bool = False, hard: Sequence[str] = (),
           pk_soft: bool = False, prefix: bool = False) -> Expect:
    """declares what `table` (or every table starting with it, prefix=True) must satisfy whenever it is committed;
    call it at import time, next to the table name"""
    e = Expect(tuple(pk), tuple(not_null), dict(ranges or {}), {k: tuple(v) for k, v in (allowed or {}).items()},
               allow_empty, tuple(hard), pk_soft)
    (_PREFIXES if prefix else _REGISTRY)[table] = e
    return e


def expectation(table: str) -> Optional[Expect]:
    """the table's declaration: by exact name, else by the longest matching prefix"""
    if table in _REGISTRY:
        return _REGISTRY[table]
    hits = [p for p in _PREFIXES if table.startswith(p)]
    return _PREFIXES[max(hits, key=len)] if hits else None


def declared() -> Dict[str, Expect]:
    return {**{p + "*": e for p, e in _PREFIXES.items()}, **_REGISTRY}


def check(df: DataFrame, table: str, pk: Optional[Sequence[str]] = None, raise_on_fail: bool = True) -> dict:
    """Runs the table's checks on `df` (what would become the live table). pk, when given (the upsert key), is checked
    too. Returns the report; raises DataQualityError when a hard check fails (and raise_on_fail)."""
    e = expectation(table) or Expect()
    # the commit's own key (the upsert key) is always hard; the declared one is hard unless pk_soft
    hard_keys = tuple(pk or ()) + (() if e.pk_soft else e.pk)
    keys = tuple(dict.fromkeys(tuple(pk or ()) + e.pk))
    cols = set(df.columns)
    failures: List[str] = []
    warnings: List[str] = []
    # a missing column fails when a hard check needs it, else warns
    needed = hard_keys + e.not_null + (tuple(e.ranges) if "ranges" in e.hard else ()) + (tuple(e.allowed) if "allowed" in e.hard else ())
    missing_hard = sorted({c for c in needed if c not in cols})
    missing_soft = sorted({c for c in keys + tuple(e.ranges) + tuple(e.allowed) if c not in cols} - set(missing_hard))
    if missing_hard:
        failures.append("missing columns %s" % missing_hard)
    if missing_soft:
        warnings.append("missing columns %s (soft checks skipped)" % missing_soft)

    null_cols = [c for c in dict.fromkeys(keys + e.not_null) if c in cols]
    aggs = [F.count(F.lit(1)).alias("__rows")]
    aggs += [F.sum(F.col(c).isNull().cast("long")).alias("__null_" + c) for c in null_cols]
    for c, (lo, hi) in e.ranges.items():
        if c in cols:
            cond = F.lit(False)
            if lo is not None:
                cond = cond | (F.col(c) < F.lit(lo))
            if hi is not None:
                cond = cond | (F.col(c) > F.lit(hi))
            aggs.append(F.sum(cond.cast("long")).alias("__range_" + c))
    for c, vals in e.allowed.items():
        if c in cols:
            aggs.append(F.sum((F.col(c).isNotNull() & ~F.col(c).isin(*vals)).cast("long")).alias("__allowed_" + c))
    row = df.agg(*aggs).first().asDict()
    rows = int(row["__rows"] or 0)

    if rows == 0 and not e.allow_empty:
        failures.append("no rows (an empty result would replace the table)")
    nulls = {c: int(row["__null_" + c] or 0) for c in null_cols}
    for c, n in nulls.items():
        if n:
            msg = "%d null value(s) in %s %s" % (n, "key column" if c in keys else "required column", c)
            (warnings if c in keys and c not in hard_keys and c not in e.not_null else failures).append(msg)
    dup = 0
    key_cols = [c for c in keys if c in cols]
    # duplicates: on the hard key (fails), and on a declared key still being confirmed (warns), each counted once
    groups = []
    hard_cols = [c for c in dict.fromkeys(hard_keys) if c in cols]
    if hard_cols:
        groups.append((hard_cols, failures, ""))
    soft_cols = [c for c in e.pk if c in cols] if e.pk_soft else []
    if soft_cols and soft_cols != hard_cols:
        groups.append((soft_cols, warnings, " (key not confirmed yet)"))
    for group, sink, note in groups if rows else []:
        n = df.groupBy(*group).count().filter(F.col("count") > 1).count()
        if n:
            dup = max(dup, n)
            sink.append("%d duplicated key(s) on (%s)%s" % (n, ", ".join(group), note))
    for kind, prefix, specs in (("ranges", "__range_", e.ranges), ("allowed", "__allowed_", e.allowed)):
        for c in specs:
            n = int(row.get(prefix + c) or 0)
            if n:
                msg = "%d value(s) of %s outside %s %s" % (n, c, "range" if kind == "ranges" else "the allowed set", specs[c])
                (failures if kind in e.hard else warnings).append(msg)

    report = {"table": table, "rows": rows, "pk": list(key_cols), "nulls": nulls, "duplicated_keys": dup,
              "passed": not failures, "failures": failures, "warnings": warnings,
              "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"}
    for w in warnings:
        print("[quality] %s: warning: %s" % (table, w), flush=True)
    if failures and raise_on_fail:
        raise DataQualityError(table, failures, report)
    return report

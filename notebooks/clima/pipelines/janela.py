# Shared infrastructure of the two climate extractors:
# date-window planning, run state (ctl_clima_extracao), municipality centroids, watermark and sampling helpers.

from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from lakehouse import core as C

STATE_TABLE = "ctl_clima_extracao"
MIN_ROWS_DAY = 5000  # a "complete" day = at least ~90% of the 5,571 municipalities
MODES = ("incremental", "range", "backfill")


def ctl(db: str) -> C.Ctl:
    return C.Ctl(db, state_table=STATE_TABLE)


def plan_window(mode: str, first_date: str, reference_date: str, start_date: str, end_date: str, lookback_days: int,
                max_days: int, reset_cursor: bool, watermark: Optional[str], state: Optional[dict]) -> dict:
    """incremental: [min(watermark+1, end-lookback+1), end]
    range:       exactly [start_date, end_date]
    backfill:    walks [start_date|first_date, end_date|end] with a persisted CURSOR, max_days calendar days per run
    (end = end_date | reference_date - 1)."""
    ref = reference_date or C.today_local()
    default_end = C.add_days(ref, -1)
    if mode == "range":
        if not start_date or not end_date:
            raise ValueError("mode=range exige start_date e end_date")
        start = max(start_date, first_date)
        return dict(start=start, end=end_date, candidates=C.days_between(start, end_date) if start <= end_date else [],
                    window_start=start, window_end=end_date, cursor_after=None, has_more_window=False)
    if mode == "incremental":
        end = end_date or default_end
        start = start_date or C.add_days(end, -(lookback_days - 1))
        if not start_date and watermark:
            start = min(start, C.add_days(watermark, 1))
        start = max(start, first_date)
        return dict(start=start, end=end, candidates=C.days_between(start, end) if start <= end else [],
                    window_start=start, window_end=end, cursor_after=None, has_more_window=False)
    ini = max(start_date or first_date, first_date)
    fim = end_date or default_end
    frm = ini
    if state and not reset_cursor and state.get("janela_inicio") == ini and state.get("janela_fim") == fim and state.get("cursor_data"):
        frm = C.add_days(str(state["cursor_data"]), 1)
    if frm > fim:
        return dict(start=frm, end=fim, candidates=[], window_start=ini, window_end=fim, cursor_after=fim, has_more_window=False)
    to = min(C.add_days(frm, max_days - 1), fim)
    return dict(start=frm, end=to, candidates=C.days_between(frm, to), window_start=ini, window_end=fim, cursor_after=to,
                has_more_window=to < fim)


def chunks(start: str, end: str, size: int) -> List[List[str]]:
    """[start, end] in blocks of at most `size` days"""
    out, a = [], start
    while a <= end:
        b = min(C.add_days(a, size - 1), end)
        out.append([a, b])
        a = C.add_days(b, 1)
    return out


def ref_db_for(ref_db: str, target_db: str, table: str = "dim_municipiosIbge") -> str:
    for db in [d for d in (ref_db, target_db) if d]:
        if C.table_exists(db, table):
            return db
    raise RuntimeError("%s não encontrada em %s — rode antes o pipeline ibge (python3 run.py ibge target_db=...)"
                       % (table, [d for d in (ref_db, target_db) if d]))


def fetch_points(db: str) -> pd.DataFrame:
    """municipality centroids (cod, lat, lon) from <db>.dim_municipiosIbge"""
    pdf = C.read_table_pd(db, "dim_municipiosIbge", ["cod_ibge", "latitude", "longitude"]).dropna()
    pts = pd.DataFrame({"cod": pdf["cod_ibge"].astype(str), "lat": pdf["latitude"].astype("float64"),
                        "lon": pdf["longitude"].astype("float64")}).reset_index(drop=True)
    if len(pts) < MIN_ROWS_DAY:
        raise RuntimeError("dim_municipiosIbge suspeita: %d pontos" % len(pts))
    return pts


def complete_days(db: str, table: str, start: Optional[str] = None, end: Optional[str] = None, count_col: Optional[str] = None) -> Set[str]:
    """days with at least MIN_ROWS_DAY rows (non-null count_col when given), read only from the year partitions involved"""
    if not C.table_exists(db, table):
        return set()
    anos = [int(a) for a in C.partitions(db, table, "ano")]
    if start and end:
        anos = [a for a in anos if int(start[:4]) <= a <= int(end[:4])]
    if not anos:
        return set()
    cols = ["data"] + ([count_col] if count_col else [])
    pdf = C.read_table_pd(db, table, cols, filters=[("ano", "in", anos)])
    if count_col:
        pdf = pdf[pdf[count_col].notna()]
    d = pd.to_datetime(pdf["data"]).dt.strftime("%Y-%m-%d")
    if start and end:
        d = d[(d >= start) & (d <= end)]
    vc = d.value_counts()
    return set(vc[vc >= MIN_ROWS_DAY].index)


def watermark(db: str, table: str, count_col: Optional[str] = None) -> Optional[str]:
    """last complete day, looking at the most recent year partitions first"""
    if not C.table_exists(db, table):
        return None
    for ano in reversed(C.partitions(db, table, "ano")):
        days = complete_days(db, table, "%s-01-01" % ano, "%s-12-31" % ano, count_col)
        if days:
            return max(days)
    return None


def js_round(x, decimals: int):
    """Math.round(x * 10^d) / 10^d (half rounds up; numpy rounds half to even)"""
    f = 10.0 ** decimals
    return np.floor(np.asarray(x, dtype=np.float64) * f + 0.5) / f


def pdf_to_df(spark: SparkSession, pdf: pd.DataFrame, fonte: str):
    """pandas → Spark through a local Parquet file (much faster than createDataFrame); returns (df, path to clean up)"""
    import os
    path = C.raw_dir(fonte, "batch") + ".parquet"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pdf.to_parquet(path, index=False)
    return spark.read.parquet("file://" + path), path


def summary_state(plan: Dict, cursor) -> Dict:
    return {"cursor_data": cursor, "janela_inicio": plan["window_start"], "janela_fim": plan["window_end"]}

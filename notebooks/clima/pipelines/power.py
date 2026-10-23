# NASA POWER (diário).
# Free public API, no key: daily meteorology on a 0.5° x 0.625° grid (~55 km, MERRA-2 / GEOS-IT), since 1981.
# The REGIONAL endpoint returns every grid point of a block (up to 10° x 10°) for a whole date range, so Brazil costs
# ~13 requests per parameter whether the range is a day or a year. Each IBGE municipality gets the nearest grid point.
# The last ~2 months are provisional (GEOS-IT) and revised when MERRA-2 comes out, so the incremental reloads the whole
# lookback window (75 days) without skipping days already loaded.
# Table <target_db>.nasa_power_diario, partitioned by year, upsert by (data, cod_ibge). Partial update:
# loading only some parameters keeps the values already stored for the others.

import time
import urllib.parse
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import core as C

from . import janela as J

FONTE = "power"
TABLE = "nasa_power_diario"
FIRST_DATE = "1981-01-01"
API = "https://power.larc.nasa.gov/api/temporal/daily/regional"
HTTP_TIMEOUT_S = 300
PAUSA_S = 1.5
BYTES_PER_ROW_PARAM = 7  # measured ~4.9 B/row of Parquet with one parameter, with a margin

PARAM_COL = {"PRECTOTCORR": "prec_mm", "T2M": "t2m_c", "T2M_MAX": "t2m_max_c", "T2M_MIN": "t2m_min_c", "RH2M": "rh2m_pct",
             "WS2M": "ws2m_ms"}
PARAM_TYPE = {"prec_mm": "decimal(8,2)", "t2m_c": "decimal(6,2)", "t2m_max_c": "decimal(6,2)", "t2m_min_c": "decimal(6,2)",
              "rh2m_pct": "decimal(6,2)", "ws2m_ms": "decimal(6,2)"}
SCHEMA = ([("data", "date"), ("cod_ibge", "string")] + [(c, PARAM_TYPE[c]) for c in PARAM_COL.values()]
          + [("pixel_lat", "decimal(9,4)"), ("pixel_lon", "decimal(9,4)"), ("loaded_at", "timestamp"), ("ano", "int")])


def build_tiles(pts: pd.DataFrame, tile_deg: int) -> List[dict]:
    """tile_deg x tile_deg blocks holding at least one municipality (1° margin); the API wants >= 2° per axis"""
    lat_min, lat_max = int(np.floor(pts["lat"].min() - 1)), int(np.ceil(pts["lat"].max() + 1))
    lon_min, lon_max = int(np.floor(pts["lon"].min() - 1)), int(np.ceil(pts["lon"].max() + 1))
    tiles = []
    for a in range(lat_min, lat_max, tile_deg):
        for b in range(lon_min, lon_max, tile_deg):
            t = {"latMin": a, "latMax": min(a + tile_deg, lat_max), "lonMin": b, "lonMax": min(b + tile_deg, lon_max)}
            if t["latMax"] - t["latMin"] < 2:
                t["latMin"] = t["latMax"] - 2
            if t["lonMax"] - t["lonMin"] < 2:
                t["lonMin"] = t["lonMax"] - 2
            hit = ((pts["lat"] >= t["latMin"] - 1) & (pts["lat"] <= t["latMax"] + 1)
                   & (pts["lon"] >= t["lonMin"] - 1) & (pts["lon"] <= t["lonMax"] + 1)).any()
            if hit:
                tiles.append(t)
    return tiles


def power_request(t: dict, params: List[str], start: str, end: str, api: str = API) -> dict:
    qs = urllib.parse.urlencode({"latitude-min": t["latMin"], "latitude-max": t["latMax"], "longitude-min": t["lonMin"],
                                 "longitude-max": t["lonMax"], "parameters": ",".join(params), "community": "AG",
                                 "start": start.replace("-", ""), "end": end.replace("-", ""), "format": "JSON", "time-standard": "LST"})
    # 403/429 stop everything and other 4xx are fatal (invalid block/parameter/dates); 5xx and network errors retry
    return C.http_json("%s?%s" % (api, qs), headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT_S)


GridStore = Dict[str, dict]  # "lat|lon" → {lat, lon, vals: {param: {AAAAMMDD: value}}}


def ingest(store: GridStore, js: dict, params: List[str]) -> int:
    fill = float((js.get("header") or {}).get("fill_value", -999))
    feats = js.get("features") if isinstance(js.get("features"), list) else ([js] if js.get("type") == "Feature" else [])
    for f in feats:
        coords = (f.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        key = "%.4f|%.4f" % (lat, lon)
        node = store.setdefault(key, {"lat": lat, "lon": lon, "vals": {}})
        par = (f.get("properties") or {}).get("parameter") or {}
        for p in params:
            series = par.get(p)
            if not series:
                continue
            dst = node["vals"].setdefault(p, {})
            for d, v in series.items():
                try:
                    x = float(v)
                except (TypeError, ValueError):
                    continue
                if np.isfinite(x) and x != fill and x > -900:
                    dst[d] = x
    return len(feats)


def nearest(nodes: List[dict], pts: pd.DataFrame) -> np.ndarray:
    """municipality → index of the nearest grid node (cosine-corrected distance), -1 when farther than ~1°"""
    if not nodes:
        return np.full(len(pts), -1)
    nl = np.array([n["lat"] for n in nodes])
    no = np.array([n["lon"] for n in nodes])
    out = np.empty(len(pts), dtype=np.int64)
    for k, (lat, lon) in enumerate(zip(pts["lat"].to_numpy(), pts["lon"].to_numpy())):
        d = (nl - lat) ** 2 + ((no - lon) * np.cos(lat * np.pi / 180)) ** 2
        b = int(np.argmin(d))
        out[k] = b if d[b] <= 1 else -1
    return out


def rows_for_block(bs: str, be: str, pts: pd.DataFrame, store: GridStore, params: List[str]) -> pd.DataFrame:
    """one row per (day, municipality) with a grid node; days where every parameter is still a fill value are dropped.
    Built as (nodes x days) matrices so a 366-day block never becomes millions of Python objects."""
    nodes = list(store.values())
    idx = nearest(nodes, pts)
    sem = int((idx < 0).sum())
    if sem:
        print("[POWER] aviso: %d municípios sem ponto de grade a menos de 1°" % sem)
    days = C.days_between(bs, be)
    dpos = {d.replace("-", ""): k for k, d in enumerate(days)}
    has = idx >= 0
    mi = idx[has]
    cols = {}
    for p in params:
        m = np.full((len(nodes), len(days)), np.nan)
        for r, node in enumerate(nodes):
            for dk, v in node["vals"].get(p, {}).items():
                c = dpos.get(dk)
                if c is not None:
                    m[r, c] = v
        cols[PARAM_COL[p]] = J.js_round(m[mi].T.ravel(), 2)  # day-major: all municipalities of day 1, then day 2...
    nd, nm = len(days), len(mi)
    pdf = pd.DataFrame({"data": np.repeat(np.array(days, dtype=object), nm), "cod_ibge": np.tile(pts["cod"].to_numpy()[has], nd)})
    for c, v in cols.items():
        pdf[c] = v
    pdf["pixel_lat"] = np.tile(J.js_round(np.array([nodes[i]["lat"] for i in mi]), 4), nd)
    pdf["pixel_lon"] = np.tile(J.js_round(np.array([nodes[i]["lon"] for i in mi]), 4), nd)
    keep = ~np.all(np.isnan(np.column_stack([cols[c] for c in cols])), axis=1)
    return pdf[keep].reset_index(drop=True)


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", reference_date: str = "", start_date: str = "",
         end_date: str = "", lookback_days: int = 75, max_days: int = 366, parametros: Optional[List[str]] = None,
         params_por_requisicao: int = 1, tile_deg: int = 10, reset_cursor: bool = False, ignorar_cota: bool = False, dry_run: bool = False,
         target_db: str = "estudo", ref_db: str = "", api: str = API):
    t0 = time.time()
    for k, v in (("reference_date", reference_date), ("start_date", start_date), ("end_date", end_date)):
        C.check_date(k, v)
    if mode not in J.MODES:
        raise ValueError("mode inválido: %s" % mode)
    if max_days < 1 or lookback_days < 1:
        raise ValueError("max_days e lookback_days devem ser >= 1")
    params = list(dict.fromkeys(str(p).strip().upper() for p in (parametros or ["PRECTOTCORR"]) if str(p).strip()))
    bad = [p for p in params if p not in PARAM_COL]
    if not params or bad:
        raise ValueError("parametros inválidos: %s; aceitos: %s" % (",".join(bad) or "(vazio)", ",".join(PARAM_COL)))
    if not 2 <= tile_deg <= 10:
        raise ValueError("tile_deg deve estar entre 2 e 10")

    ctl = J.ctl(target_db)
    wm = J.watermark(target_db, TABLE, count_col="prec_mm")  # last day with precipitation for >= MIN_ROWS_DAY municipalities
    ref = reference_date or C.today_local()
    default_end = C.add_days(ref, -1)
    cursor_after, has_more_window = None, False
    if mode == "range":
        if not start_date or not end_date:
            raise ValueError("mode=range exige start_date e end_date")
        win_start, win_end = max(start_date, FIRST_DATE), end_date
        blocks = J.chunks(win_start, win_end, max_days)
    elif mode == "incremental":
        win_end = end_date or default_end
        win_start = start_date or C.add_days(win_end, -(lookback_days - 1))
        if not start_date and wm:
            win_start = min(win_start, C.add_days(wm, 1))
        win_start = max(win_start, FIRST_DATE)
        blocks = J.chunks(win_start, win_end, max_days) if win_start <= win_end else []
    else:
        win_start, win_end = max(start_date or FIRST_DATE, FIRST_DATE), end_date or default_end
        st = ctl.read_state(FONTE, "backfill")
        frm = win_start
        if st and not reset_cursor and st.get("janela_inicio") == win_start and st.get("janela_fim") == win_end and st.get("cursor_data"):
            frm = C.add_days(str(st["cursor_data"]), 1)
        if frm > win_end:
            blocks, cursor_after = [], win_end
        else:
            to = min(C.add_days(frm, max_days - 1), win_end)
            blocks, cursor_after, has_more_window = [[frm, to]], to, to < win_end
    plan = {"window_start": win_start, "window_end": win_end}

    step = max(1, params_por_requisicao)
    groups = [params[i:i + step] for i in range(0, len(params), step)]
    janela = "%s..%s" % (win_start, win_end)
    try:
        pts = J.fetch_points(J.ref_db_for(ref_db, target_db))
    except RuntimeError as e:
        if not dry_run:
            raise
        pts, aviso = None, str(e)  # a dry run still shows the date plan before the IBGE table exists
    tiles = build_tiles(pts, tile_deg) if pts is not None else []
    n_req = len(blocks) * len(tiles) * len(groups)
    print("[POWER] mode=%s janela %s | watermark=%s | %d bloco(s) de datas x %d blocos de grade x %d grupo(s) de parâmetros = %d requisições → %s.%s"
          % (mode, janela, wm, len(blocks), len(tiles), len(groups), n_req, target_db, TABLE))
    n_days = sum(len(C.days_between(a, b)) for a, b in blocks)
    estimate = n_days * 5571 * BYTES_PER_ROW_PARAM * len(PARAM_COL if C.table_exists(target_db, TABLE) else params)
    if dry_run:
        out = {"fonte": FONTE, "mode": mode, "dry_run": True, "janela": janela, "watermark": wm, "blocos_datas": blocks,
               "cota": C.quota_check(estimate),
               "blocos_grade": len(tiles), "parametros": params, "requisicoes": n_req, "has_more": has_more_window}
        if pts is None:
            out["aviso"] = aviso
        return out
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")

    C.require_space(estimate, "NASA POWER %s" % janela, ignorar_cota, "Use uma janela menor (max_days) ou menos parâmetros.")
    tmp: List[str] = []
    try:
        dias, req, n = set(), 0, 0
        loaded_cols = [PARAM_COL[p] for p in params]
        keep_cols = [c for c in PARAM_COL.values() if c not in loaded_cols]
        for bs, be in blocks:
            store: GridStore = {}
            for t in tiles:
                for g in groups:
                    req += 1
                    js = power_request(t, g, bs, be, api)
                    nf = ingest(store, js, g)
                    print("[POWER] %d/%d %s..%s lat %s..%s lon %s..%s [%s]: %d pontos"
                          % (req, n_req, bs, be, t["latMin"], t["latMax"], t["lonMin"], t["lonMax"], ",".join(g), nf))
                    time.sleep(PAUSA_S)
            pdf = rows_for_block(bs, be, pts, store, params)
            if pdf.empty:
                print("[POWER] bloco %s..%s: nenhum dia com dado" % (bs, be))
                continue
            dias.update(pdf["data"].unique())
            pdf["ano"] = pdf["data"].str[:4].astype(int)
            df, path = J.pdf_to_df(spark, pdf, FONTE)
            tmp.append(path)
            df = df.withColumn("data", F.to_date("data"))
            if keep_cols and C.table_exists(target_db, TABLE):  # partial update: keep the parameters not loaded now
                old = (spark.read.parquet(C.uri(C.table_path(target_db, TABLE)))
                       .filter(F.col("ano").isin(sorted(pdf["ano"].unique().tolist())))
                       .filter((F.col("data") >= F.lit(bs).cast("date")) & (F.col("data") <= F.lit(be).cast("date")))
                       .select("data", "cod_ibge", *keep_cols))
                df = df.join(old, ["data", "cod_ibge"], "left")
            for c in keep_cols:
                if c not in df.columns:
                    df = df.withColumn(c, F.lit(None).cast("double"))
            n += C.stage(C.conform(df, SCHEMA), spark, target_db, TABLE)
            C.commit_partitions(spark, target_db, TABLE, "ano", pk=["data", "cod_ibge"])
            print("[POWER] bloco %s..%s: %d pontos de grade, %d linhas" % (bs, be, len(store), len(pdf)))

        dias_l = sorted(dias)
        cursor = cursor_after if mode == "backfill" else (dias_l[-1] if dias_l else None)
        result = {"fonte": FONTE, "mode": mode, "janela": janela, "blocos_datas": blocks, "parametros": params,
                  "watermark_anterior": wm, "requisicoes": req, "dias_com_dado": len(dias_l),
                  "primeiro_dia": dias_l[0] if dias_l else None, "ultimo_dia_com_dado": dias_l[-1] if dias_l else None,
                  "linhas_carregadas": n, "cursor": cursor, "has_more": has_more_window,
                  "duracao_s": round(time.time() - t0), "concluido_em": C.utcnow_iso()}
        ctl.write_state(FONTE, mode, "ok", result, **J.summary_state(plan, cursor))
        print(result)
        return result
    except Exception as e:
        ctl.write_error(FONTE, mode, e)  # cursor/window untouched: the backfill resumes from the same point
        raise
    finally:
        C.cleanup(*tmp)

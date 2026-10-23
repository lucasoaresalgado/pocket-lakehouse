# INPE/CPTEC MERGE (precipitação diária).
# Daily rain on a 0.1° grid (~11 km) over South America (GPM/IMERG satellite + rain gauges), one GRIB2 per day published
# on D+1. The MERGE day is the 12 UTC D-1 → 12 UTC D accumulation, stored under the file date. Each IBGE municipality
# (centroid from dim_municipiosIbge) gets the nearest pixel (prec_mm) and the 3x3 block mean around it (prec_mm_3x3, ≈33 km).
# Table <target_db>.cptec_merge_diario, partitioned by year, upsert by (data, cod_ibge).

import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import core as C

from . import grib2
from . import janela as J

FONTE = "merge"
TABLE = "cptec_merge_diario"
FIRST_DATE = "2000-06-01"
URL_TEMPLATE = "https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/DAILY/{Y}/{m}/MERGE_CPTEC_{Y}{m}{d}.grib2"
HTTP_TIMEOUT_S = 180
BYTES_PER_ROW = 10  # measured ~7.2 B/row of Parquet (5,571 rows/day), with a margin

SCHEMA = [("data", "date"), ("cod_ibge", "string"), ("prec_mm", "decimal(8,2)"), ("prec_mm_3x3", "decimal(8,2)"),
          ("pixel_lat", "decimal(9,4)"), ("pixel_lon", "decimal(9,4)"), ("arquivo", "string"), ("loaded_at", "timestamp"), ("ano", "int")]


def day_url(d: str, template: str = URL_TEMPLATE) -> str:
    y, m, dd = d.split("-")
    return template.replace("{Y}", y).replace("{m}", m).replace("{d}", dd)


def sanitize(a: np.ndarray) -> np.ndarray:
    """values < 0 (missing/ocean) or > 2000 mm/day become NaN"""
    a = a.copy()
    a[~np.isfinite(a) | (a < 0) | (a > 2000)] = np.nan
    return a


def sample_points(g: grib2.Grid, pts: pd.DataFrame) -> pd.DataFrame:
    """nearest pixel + mean of the finite values of the 3x3 block (vectorized samplePoints())"""
    vals = g.values.reshape(g.nlat, g.nlon)
    i = np.floor((pts["lat"].to_numpy() - g.lat0) / g.dlat + 0.5).astype(np.int64)
    j = np.floor((pts["lon"].to_numpy() - g.lon0) / g.dlon + 0.5).astype(np.int64)
    inside = (i >= 0) & (i < g.nlat) & (j >= 0) & (j < g.nlon)
    ic, jc = np.clip(i, 0, g.nlat - 1), np.clip(j, 0, g.nlon - 1)
    v = vals[ic, jc]
    s = np.zeros(len(pts))
    n = np.zeros(len(pts))
    for a in (-1, 0, 1):
        for b in (-1, 0, 1):
            ia, jb = ic + a, jc + b
            ok = (ia >= 0) & (ia < g.nlat) & (jb >= 0) & (jb < g.nlon)
            x = vals[np.clip(ia, 0, g.nlat - 1), np.clip(jb, 0, g.nlon - 1)]
            good = ok & np.isfinite(x)
            s += np.where(good, x, 0)
            n += good
    with np.errstate(invalid="ignore", divide="ignore"):
        m = np.where(n > 0, s / np.maximum(n, 1), np.nan)
    out = pd.DataFrame({"cod_ibge": pts["cod"].to_numpy(),
                        "prec_mm": np.where(inside & np.isfinite(v), J.js_round(v, 2), np.nan),
                        "prec_mm_3x3": np.where(inside, J.js_round(m, 2), np.nan),
                        "pixel_lat": np.where(inside, J.js_round(g.lat0 + i * g.dlat, 4), np.nan),
                        "pixel_lon": np.where(inside, J.js_round(g.lon0 + j * g.dlon, 4), np.nan)})
    return out


def fetch_day(url: str) -> Optional[bytes]:
    """None = not published yet (404)"""
    try:
        return C.http_bytes(url, timeout=HTTP_TIMEOUT_S)
    except C.FatalHttp as e:
        if e.code == 404:
            return None
        raise


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", reference_date: str = "", start_date: str = "",
         end_date: str = "", lookback_days: int = 7, max_days: int = 120, concurrency: int = 2, force: bool = False,
         reset_cursor: bool = False, ignorar_cota: bool = False, dry_run: bool = False, target_db: str = "estudo", ref_db: str = "",
         url_template: str = URL_TEMPLATE):
    t0 = time.time()
    for k, v in (("reference_date", reference_date), ("start_date", start_date), ("end_date", end_date)):
        C.check_date(k, v)
    if mode not in J.MODES:
        raise ValueError("mode inválido: %s" % mode)
    if max_days < 1 or lookback_days < 1:
        raise ValueError("max_days e lookback_days devem ser >= 1")
    ctl = J.ctl(target_db)
    wm = J.watermark(target_db, TABLE)
    plan = J.plan_window(mode, FIRST_DATE, reference_date, start_date, end_date, lookback_days, max_days, reset_cursor, wm,
                         ctl.read_state(FONTE, "backfill") if mode == "backfill" else None)
    done = J.complete_days(target_db, TABLE, plan["start"], plan["end"]) if not force and plan["candidates"] else set()
    pending = [d for d in plan["candidates"] if d not in done]
    batch = pending if mode == "backfill" else pending[:max_days]
    janela = "%s..%s" % (plan["start"], plan["end"])
    print("[MERGE] mode=%s janela %s | watermark=%s | %d dias, %d já carregados, %d a processar → %s.%s"
          % (mode, janela, wm, len(plan["candidates"]), len(done), len(batch), target_db, TABLE))
    estimate = len(batch) * 5571 * BYTES_PER_ROW
    if dry_run:
        return {"fonte": FONTE, "mode": mode, "dry_run": True, "janela": janela, "watermark": wm, "a_processar": batch,
                "cota": C.quota_check(estimate),
                "has_more": plan["has_more_window"] if mode == "backfill" else len(pending) > len(batch)}
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")

    C.require_space(estimate, "MERGE %s" % janela, ignorar_cota, "Use uma janela menor (max_days).")
    tmp: List[str] = []
    try:
        pts = J.fetch_points(J.ref_db_for(ref_db, target_db))
        ok, missing, errors, frames = [], [], [], []

        def work(d: str):
            url = day_url(d, url_template)
            try:
                raw = fetch_day(url)
                if raw is None:
                    print("[MERGE] %s: não publicado (404)" % d)
                    return d, "missing", None
                msg = grib2.pick_precip(grib2.decode(raw))
                g = msg.grid._replace(values=sanitize(msg.grid.values))
                s = sample_points(g, pts)
                s.insert(0, "data", d)
                s["arquivo"] = url.rsplit("/", 1)[-1]
                print("[MERGE] %s: grade %dx%d (5.%d), %d/%d municípios com valor"
                      % (d, g.nlat, g.nlon, msg.tpl_data, int(s["prec_mm"].notna().sum()), len(s)))
                return d, "ok", s
            except C.FatalHttp as e:
                if e.code in (403, 429):
                    raise  # stop everything: the server refused / rate-limited us
                return d, "erro", "%s: %s" % (d, e)
            except Exception as e:
                print("[MERGE] %s: ERRO %s" % (d, e))
                return d, "erro", "%s: %s" % (d, e)

        with ThreadPoolExecutor(max_workers=max(1, min(concurrency, len(batch) or 1))) as ex:
            for d, st, payload in ex.map(work, batch):
                if st == "ok":
                    ok.append(d)
                    frames.append(payload)
                elif st == "missing":
                    missing.append(d)
                else:
                    errors.append(payload)

        n = 0
        if frames:
            pdf = pd.concat(frames, ignore_index=True)
            pdf["ano"] = pdf["data"].str[:4].astype(int)
            df, path = J.pdf_to_df(spark, pdf, FONTE)
            tmp.append(path)
            n = C.stage(C.conform(df.withColumn("data", F.to_date("data")), SCHEMA), spark, target_db, TABLE)
            C.commit_partitions(spark, target_db, TABLE, "ano", pk=["data", "cod_ibge"])

        # has_more only when the next run really moves on (avoids an endless loop on 404/force)
        has_more = not errors and (plan["has_more_window"] if mode == "backfill"
                                   else (not force and len(pending) > len(batch) and len(ok) > 0))
        if mode == "backfill":
            cursor = C.add_days(plan["start"], -1) if errors else plan["cursor_after"]
        else:
            cursor = max(ok) if ok else None
        result = {"fonte": FONTE, "mode": mode, "janela": janela, "watermark_anterior": wm, "dias_planejados": len(batch),
                  "dias_carregados": len(ok), "dias_nao_publicados": sorted(missing), "erros": errors, "linhas_carregadas": n,
                  "cursor": cursor, "has_more": has_more, "duracao_s": round(time.time() - t0), "concluido_em": C.utcnow_iso()}
        status = ("parcial" if ok else "erro") if errors else "ok"
        ctl.write_state(FONTE, mode, status, result, **J.summary_state(plan, cursor))
        print(result)
        if errors:
            raise RuntimeError("%d dia(s) com erro: %s" % (len(errors), " | ".join(errors[:5])))
        return result
    except Exception as e:
        if "dia(s) com erro" not in str(e):
            ctl.write_error(FONTE, mode, e)
        raise
    finally:
        C.cleanup(*tmp)

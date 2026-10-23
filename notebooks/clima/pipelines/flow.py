# Flow - Clima/Chuva (MERGE CPTEC + NASA POWER) incremental/backfill.
# Two independent branches (skip_failure: one failing does not stop the other), each a while loop until has_more=false,
# then a Summary step with one row per branch. MERGE: max_days 120, lookback 7. POWER: max_days 366, lookback 75.
# If dim_municipiosIbge is not in the lake yet, the ibge pipeline runs first.

import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from pyspark.sql import SparkSession

from lakehouse import core as C

from . import ibge, merge, power

MAX_ITERACOES = 400  # safety latch of the while loops


def _resumo(branch) -> dict:
    """one summary row per branch (the last iteration of its loop)"""
    it = branch[-1] if isinstance(branch, list) and branch else branch
    if not isinstance(it, dict):
        return {"erro": str(branch)}
    if it.get("error"):
        return {"erro": str(it["error"])[:500]}
    out = {"fonte": it.get("fonte"), "mode": it.get("mode"), "janela": it.get("janela"), "linhas": it.get("linhas_carregadas"),
           "cursor": it.get("cursor"), "has_more": it.get("has_more"), "erros": it.get("erros") or None,
           "iteracoes": len(branch) if isinstance(branch, list) else 1}
    if it.get("dry_run"):
        out["status"] = "dry_run"
        out["aviso"] = it.get("aviso")
    return out


def run_flow(spark: Optional[SparkSession] = None, mode: str = "incremental", reference_date: str = "", start_date: str = "",
             end_date: str = "", force: bool = False, target_db: str = "estudo", ref_db: str = "",
             power_parametros: Optional[List[str]] = None, dry_run: bool = False, parallel: bool = False) -> List[dict]:
    if mode not in ("incremental", "range", "backfill"):
        raise ValueError("mode inválido: %s" % mode)
    target_db = target_db or "estudo"
    janela = dict(mode=mode, reference_date=reference_date or "", start_date=start_date or "", end_date=end_date or "")
    rows = []
    if not any(C.table_exists(d, ibge.TB_MUN) for d in (ref_db, target_db) if d):
        ref_db = ref_db or target_db
        r = ibge.main(spark, dry_run=dry_run, target_db=ref_db)
        rows.append({"ramo": "pré-requisito: IBGE municípios", "fonte": ibge.FONTE, "status": "dry_run" if dry_run else r.get("status"),
                     "linhas": r.get("municipios_carregados", r.get("municipios"))})

    def loop(fn, **kw):
        iteracoes = []
        while len(iteracoes) < MAX_ITERACOES:  # stop_after_if: !result.has_more
            r = fn(spark, **janela, **kw, reset_cursor=False, dry_run=dry_run, target_db=target_db, ref_db=ref_db)
            iteracoes.append(r)
            if dry_run or not r.get("has_more"):
                break
        return iteracoes

    branches = {
        "MERGE CPTEC": lambda: loop(merge.main, force=force, max_days=120, lookback_days=7, concurrency=2),
        "NASA POWER": lambda: loop(power.main, max_days=366, lookback_days=75, tile_deg=10, params_por_requisicao=1,
                                   parametros=power_parametros or ["PRECTOTCORR"]),
    }

    def safe(name):
        try:
            return branches[name]()
        except Exception as e:  # skip_failure
            traceback.print_exc()
            return {"error": "%s: %s" % (name, e)}

    if parallel:
        with ThreadPoolExecutor(max_workers=2) as ex:
            results = list(ex.map(safe, branches))
    else:
        results = [safe(n) for n in branches]
    return rows + [dict(ramo=n, **_resumo(r)) for n, r in zip(branches, results)]

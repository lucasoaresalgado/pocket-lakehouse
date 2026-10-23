# Mining & oil flow (ANM CFEM, AMB, SCM, SIGBM, DIPEM + ANP well production), incremental/backfill.

import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

from pyspark.sql import SparkSession

from . import amb, anp, cfem, dipem, scm, sigbm

MAX_ITERACOES_ANP = 200  # safety stop for the ANP while loop


def _resumo(branch) -> dict:
    """the flow's "Summary" step: most branches return one dict, ANP returns a list of iterations (the last one counts)"""
    it = branch[-1] if isinstance(branch, list) and branch else branch
    if not isinstance(it, dict):
        return {"erro": str(branch)}
    if it.get("error"):
        err = it["error"]
        return {"erro": str(err.get("message", err) if isinstance(err, dict) else err)[:500]}
    out = {"fonte": it.get("fonte"), "mode": it.get("mode"), "status": it.get("status") or ("dry_run" if it.get("dry_run") else None),
           "janela": it.get("janela"), "linhas": it.get("linhas_carregadas"), "cursor": it.get("cursor"), "has_more": it.get("has_more")}
    if it.get("dry_run"):  # extras only a dry run has: did the source answer and does its layout still map?
        v = it.get("verificacao_fonte")
        if v is not None:
            ruins = [x.get("erro") or "faltam %s" % x.get("faltam") for x in v if not x.get("layout_ok")]
            out["fonte_ok"] = "ok" if v and not ruins else "; ".join(map(str, ruins)) or "not checked"
        else:
            out["fonte_ok"] = "%d file(s) in the batch" % len(it.get("arquivos_do_lote", []))
        if it.get("aviso"):
            out["aviso"] = it["aviso"]
    return out


def run_flow(spark: Optional[SparkSession] = None, mode: str = "incremental", reference_date: str = "", start_date: str = "",
             end_date: str = "", force: bool = False, target_db: str = "estudo", cfg_db: str = "", dry_run: bool = False,
             parallel: bool = False) -> List[dict]:
    if mode not in ("incremental", "range", "backfill"):
        raise ValueError("invalid mode: %s" % mode)
    target_db = target_db or "estudo"
    common = dict(spark=spark, force=force, dry_run=dry_run, target_db=target_db, cfg_db=cfg_db or "")
    janela = dict(mode=mode, reference_date=reference_date or "", start_date=start_date or "", end_date=end_date or "")
    snap_mode = "backfill" if mode == "backfill" else "incremental"

    def b_cfem():
        return cfem.main(**common, **janela, lookback_meses=6, incluir_p3=True, profile=False)

    def b_amb():
        return amb.main(**common, **janela, incluir_p3=True, profile=False)

    def b_anp():
        iteracoes = []
        while len(iteracoes) < MAX_ITERACOES_ANP:  # stop_after_if: !result.has_more
            r = anp.main(**common, **janela, max_periodos=12, lookback_meses=3, reset_cursor=False, profile=False)
            iteracoes.append(r)
            if dry_run or not r.get("has_more"):
                break
        return iteracoes

    def b_scm():
        return scm.main(**common, mode=snap_mode, incluir_p3=True, profile=False)

    def b_sigbm():
        return sigbm.main(**common, mode=snap_mode, incluir_p3=True, profile=False)

    def b_dipem():
        return dipem.main(**common, mode=snap_mode, incluir_p3=True, profile=False)

    branches: Dict[str, Callable] = {"ANM CFEM": b_cfem, "ANM AMB": b_amb, "ANP produção por poço": b_anp,
                                     "ANM Cadastro Mineiro (SCM)": b_scm, "ANM SIGBM barragens": b_sigbm,
                                     "ANM DIPEM investimento em pesquisa": b_dipem}

    def safe(name: str):
        try:
            return branches[name]()
        except Exception as e:  # skip_failure: the branch result becomes an error object, the others carry on
            traceback.print_exc()
            return {"error": {"message": "%s: %s" % (name, e)}}

    if parallel:
        with ThreadPoolExecutor(max_workers=len(branches)) as ex:
            results = list(ex.map(safe, branches))
    else:
        results = [safe(n) for n in branches]
    return [dict(ramo=n, **_resumo(r)) for n, r in zip(branches, results)]

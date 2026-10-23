# Flow - External Public Data (RF CNPJ → CVM → PGFN).
# Three sequential steps, each with the script defaults (latest release) and continue_on_error, so one failing source
# does not block the others. The full CNPJ release and PGFN quarter do not fit the 5 GiB
# bucket quota, so the flow takes their study filters (cnpj_ufs, pgfn_ufs); without them the size guards decide.
# The other sources of this category (CGU, MTE, PNCP, CAGED) run on their own.

import traceback
from typing import List, Optional

from pyspark.sql import SparkSession

from . import cnpj, cvm, pgfn


def _resumo(name: str, r) -> dict:
    if not isinstance(r, dict):
        return {"passo": name, "erro": str(r)[:500]}
    if r.get("error"):
        return {"passo": name, "erro": str(r["error"])[:500]}
    out = {"passo": name, "fonte": r.get("fonte"), "success": r.get("success"), "dry_run": r.get("dry_run", False),
           "arquivos_ok": r.get("files_succeeded"), "arquivos_falha": r.get("files_failed")}
    for k in ("reference_date", "quarter", "years", "zip_total_gib", "aviso"):
        if r.get(k) is not None:
            out[k] = r[k]
    return out


def run_flow(spark: Optional[SparkSession] = None, target_db: str = "estudo", cnpj_ufs: Optional[List[str]] = None,
             cnpj_tables: Optional[List[str]] = None, pgfn_ufs: Optional[List[str]] = None, dry_run: bool = False) -> List[dict]:
    steps = [
        ("Receita Federal CNPJ → cnpj_*", lambda: cnpj.main(spark, reference_date="", tables=cnpj_tables, ufs=cnpj_ufs,
                                                           dry_run=dry_run, target_db=target_db)),
        ("CVM DFP/ITR → cvm_*", lambda: cvm.main(spark, years=None, doc_types=["DFP", "ITR"], dry_run=dry_run, target_db=target_db)),
        ("PGFN devedores → pgfn_devedores", lambda: pgfn.main(spark, tipos=None, quarter="", ufs=pgfn_ufs, dry_run=dry_run,
                                                             target_db=target_db)),
    ]
    out = []
    for name, fn in steps:
        try:
            r = fn()
        except Exception as e:  # continue_on_error
            traceback.print_exc()
            r = {"error": "%s: %s" % (name, e)}
        out.append(_resumo(name, r))
    return out

# CVM (Comissão de Valores Mobiliários) — financial statements of listed companies.
# DFP (annual) and/or ITR (quarterly) zips from dados.cvm.gov.br, one per year, each with one CSV per statement
# (latin-1, ';', with header). Table per statement: <target_db>.cvm_{doc_type}_{statement} (e.g. cvm_dfp_bpa_con),
# columns = the CSV header lower-cased, values as published (+ loaded_at).
# Each table is a snapshot of THIS run (a run with years=[2024, 2025] leaves exactly those years);
# here all years of a table are staged first and the table is swapped once.

import os
import re
import time
import zipfile
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import core as C

FONTE = "cvm"
BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC"
SR_KEY_COL = "cnpj_cia"


def zip_url(doc_type: str, year: int, base_url: str = BASE_URL) -> str:
    return "%s/%s/DADOS/%s_cia_aberta_%d.zip" % (base_url, doc_type.upper(), doc_type.lower(), year)


def statement_key(filename: str, doc_type: str) -> Optional[str]:
    """'dfp_cia_aberta_BPA_con_2024.csv' → 'bpa_con'; anything else → None"""
    m = re.match(r"%s(.+?)_(\d{4})\.csv$" % re.escape("%s_cia_aberta_" % doc_type.lower()), filename, re.IGNORECASE)
    return m.group(1).lower() if m else None


def sr_table_name(doc_type: str, key: str) -> str:
    return "cvm_%s_%s" % (doc_type.lower(), key)


def main(spark: Optional[SparkSession] = None, years: Optional[List[int]] = None, doc_types: Optional[List[str]] = None,
         ignorar_cota: bool = False, dry_run: bool = False, target_db: str = "estudo", base_url: str = BASE_URL):
    t0 = time.time()
    years = sorted({int(y) for y in (years or [datetime.now().year])})
    doc_types = [d.upper() for d in (doc_types or ["DFP", "ITR"])]
    unknown = [d for d in doc_types if d not in ("DFP", "ITR")]
    if unknown:
        raise ValueError("Unknown doc_types: %s. Valid: ['DFP', 'ITR']" % unknown)
    urls = [(d, y, zip_url(d, y, base_url)) for d in doc_types for y in years]
    plano = {"fonte": FONTE, "doc_types": doc_types, "years": years, "arquivos": [u for _, _, u in urls], "destino": "%s.cvm_*" % target_db}
    print("[CVM] %s" % plano)
    check = []
    for _, _, u in urls:
        try:
            fi = C.head_info(u)
            check.append({"url": u, "mb": round((fi["tamanho"] or 0) / 1048576, 1), "last_modified": fi["last_modified"]})
        except Exception as e:
            check.append({"url": u, "erro": str(e)[:200]})
        time.sleep(C.PAUSA_S)
    estimate = int(sum(c.get("mb", 0) for c in check) * 2 * 1048576)  # measured: Parquet ≈ 1-2x the zip
    if dry_run:
        return dict(plano, dry_run=True, verificacao_fonte=check, cota=C.quota_check(estimate))
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")
    C.require_space(estimate, "CVM %s %s" % (doc_types, years), ignorar_cota, "Carregue menos anos.")

    tmp: List[str] = []
    results: List[dict] = []
    files: Dict[str, List[tuple]] = {}  # table → [(label, csv path, columns)]
    try:
        for doc_type, year, url in urls:
            label = "%s/%d" % (doc_type, year)
            try:
                zpath, nbytes = C.download(url, FONTE)
                tmp.append(zpath)
                with zipfile.ZipFile(zpath) as zf:
                    members = [n for n in zf.namelist() if n.lower().endswith(".csv") and statement_key(os.path.basename(n), doc_type)]
                xdir = C.raw_dir(FONTE, "%s_%d" % (doc_type.lower(), year))
                tmp.append(xdir)
                paths = C.unzip(zpath, xdir, members)
                print("[CVM] %s: %.1f MB, CSVs %s" % (label, nbytes / 1048576, [statement_key(os.path.basename(p), doc_type) for p in paths]))
                for p in paths:
                    key = statement_key(os.path.basename(p), doc_type)
                    cols = [c.strip().lower() for c in C.split_csv(C.first_line(p, "ISO-8859-1"), ";")]
                    if SR_KEY_COL not in cols:
                        print("[CVM] %s/%s: skipping — '%s' not in header: %s" % (label, key, SR_KEY_COL, cols))
                        continue
                    files.setdefault(sr_table_name(doc_type, key), []).append(("%s/%s" % (label, key), p, cols))
            except Exception as e:
                print("[CVM] %s: ZIP download/open failed: %s" % (label, e))
                results.append({"label": label, "success": False, "error": str(e)[:500]})
            time.sleep(C.PAUSA_S)

        for table, items in sorted(files.items()):
            t1 = time.time()
            try:
                dfs = []
                for label, path, cols in items:
                    raw = C.read_csv(spark, path, ";", "ISO-8859-1", header=True)
                    dfs.append(raw.select(*[C.nz("c%d" % i).alias(c) for i, c in enumerate(cols) if i < len(raw.columns)]))
                df = dfs[0]
                for d in dfs[1:]:
                    df = df.unionByName(d, allowMissingColumns=True)
                n = C.load(spark, df.withColumn("loaded_at", F.current_timestamp()), target_db, table, snapshot=True)
                results.append({"label": table, "success": True, "files": [l for l, _, _ in items], "sr_rows": n,
                                "duration": "%ds" % (time.time() - t1)})
                print("[CVM] %s: %d rows (%s)" % (table, n, ", ".join(l for l, _, _ in items)))
            except Exception as e:
                C.drop_stage(target_db, table)
                results.append({"label": table, "success": False, "error": str(e)[:500], "duration": "%ds" % (time.time() - t1)})
    finally:
        C.cleanup(*tmp)
    failed = [r for r in results if not r["success"]]
    result = dict(plano, success=not failed, files_succeeded=len(results) - len(failed), files_failed=len(failed), results=results,
                  duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
    C.Ctl(target_db).write_state(FONTE, "snapshot", "ok" if not failed else "parcial", result)
    return result

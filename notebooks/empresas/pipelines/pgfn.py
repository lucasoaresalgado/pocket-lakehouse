# PGFN — Lista de Devedores (dívida ativa da União).
# Three quarterly zips (Não Previdenciário, FGTS, Previdenciário) from dadosabertos.pgfn.gov.br, each with several CSVs
# (latin-1, ';', with header) sharing one schema. All go into one table <target_db>.pgfn_devedores (columns as published
# + tipo_divida + loaded_at), a SNAPSHOT of the quarter. The latest quarter is discovered from the
# index page, and the table is replaced only by a run where some debt type downloaded (here: only after all of the
# requested types are staged — a failed type keeps the previous snapshot).
# Study filter: `ufs` keeps only debtors of those states (UF_DEVEDOR); the full quarter is ~1.5 GB zipped.

import re
import time
import zipfile
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import core as C

FONTE = "pgfn"
TABLE = "pgfn_devedores"
BASE_URL = "https://dadosabertos.pgfn.gov.br"
DEBT_FILENAMES = {"nao_previdenciario": "Dados_abertos_Nao_Previdenciario.zip", "fgts": "Dados_abertos_FGTS.zip",
                  "previdenciario": "Dados_abertos_Previdenciario.zip"}


def get_latest_quarter(base_url: str = BASE_URL) -> str:
    quarters = sorted(set(re.findall(r"(\d{4}_trimestre_\d{2})", C.http_text(base_url + "/"))))
    if not quarters:
        raise ValueError("Could not find any quarter directory on PGFN index page")
    return quarters[-1]


def main(spark: Optional[SparkSession] = None, tipos: Optional[List[str]] = None, quarter: str = "",
         ufs: Optional[List[str]] = None, ignorar_cota: bool = False, dry_run: bool = False, target_db: str = "estudo",
         base_url: str = BASE_URL):
    t0 = time.time()
    tipos = list(tipos or DEBT_FILENAMES)
    unknown = [t for t in tipos if t not in DEBT_FILENAMES]
    if unknown:
        raise ValueError("Unknown tipos: %s. Valid: %s" % (unknown, list(DEBT_FILENAMES)))
    ufs = sorted({str(u).upper() for u in (ufs or [])})
    quarter = quarter or get_latest_quarter(base_url)
    urls = {t: "%s/%s/%s" % (base_url, quarter, DEBT_FILENAMES[t]) for t in tipos}
    infos = {}
    for t, u in urls.items():
        try:
            infos[t] = C.head_info(u)
        except Exception as e:
            infos[t] = {"url": u, "erro": str(e)[:200], "tamanho": None}
        time.sleep(C.PAUSA_S)
    zip_total = sum(i.get("tamanho") or 0 for i in infos.values())
    guard = C.quota_check(int(zip_total * 0.9), C.table_bytes(target_db, TABLE))
    plano = {"fonte": FONTE, "quarter": quarter, "tipos": tipos, "ufs": ufs or "todas", "zip_total_gib": round(zip_total / (1 << 30), 2),
             "cota": guard, "destino": "%s.%s" % (target_db, TABLE)}
    print("[PGFN] %s" % plano)
    if dry_run:
        return dict(plano, dry_run=True, verificacao_fonte=list(infos.values()))
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")
    if not ufs and not guard["cabe"] and not ignorar_cota:
        raise RuntimeError("Carga estimada em %.1f GiB e só há %.1f GiB livres na cota: use ufs=[...] (ou ignorar_cota=True)"
                           % (guard["estimativa_gib"], guard["livre_gib"]))

    tmp: List[str] = []
    results, columns = [], None
    C.drop_stage(target_db, TABLE)
    try:
        staged = 0
        for tipo in tipos:
            t1 = time.time()
            label = "PGFN/%s" % tipo
            try:
                zpath, nbytes = C.download(urls[tipo], FONTE)
                tmp.append(zpath)
                with zipfile.ZipFile(zpath) as zf:
                    members = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
                if not members:
                    raise ValueError("No CSV files found in ZIP")
                xdir = C.raw_dir(FONTE, tipo)
                tmp.append(xdir)
                paths = C.unzip(zpath, xdir, members)
                C.cleanup(zpath)
                if columns is None:  # schema from the first file, reused for the other types
                    columns = [c.strip() for c in C.split_csv(C.first_line(paths[0], "ISO-8859-1"), ";")]
                    print("[PGFN] Columns (%d): %s" % (len(columns), columns))
                raw = C.read_csv(spark, paths, ";", "ISO-8859-1", header=True)
                df = raw.select(*[C.nz("c%d" % i).alias(c) if i < len(raw.columns) else F.lit(None).cast("string").alias(c)
                                  for i, c in enumerate(columns)])
                if ufs and "UF_DEVEDOR" in columns:
                    df = df.filter(F.col("UF_DEVEDOR").isin(*ufs))
                before = staged
                staged = C.stage(df.withColumn("tipo_divida", F.lit(tipo)).withColumn("loaded_at", F.current_timestamp()),
                                 spark, target_db, TABLE, append=True)
                C.cleanup(xdir)
                results.append({"label": label, "success": True, "tipo_divida": tipo, "arquivos_csv": len(paths),
                                "total_rows": staged - before, "duration": "%ds" % (time.time() - t1)})
                print("[PGFN] %s: %.0f MB, %d CSVs, %d rows" % (label, nbytes / 1048576, len(paths), staged - before))
            except Exception as e:
                results.append({"label": label, "success": False, "tipo_divida": tipo, "error": str(e)[:500],
                                "duration": "%ds" % (time.time() - t1)})
                print("[PGFN] %s FAILED: %s" % (label, e))
            time.sleep(C.PAUSA_S)
        failed = [r for r in results if not r["success"]]
        if failed or staged == 0:
            C.drop_stage(target_db, TABLE)  # never mix a partial quarter into the snapshot
        else:
            C.commit(spark, target_db, TABLE, snapshot=True)
    finally:
        C.cleanup(*tmp)
    result = dict(plano, success=not failed, files_succeeded=len(results) - len(failed), files_failed=len(failed),
                  linhas_carregadas=0 if failed else staged, results=results, duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
    C.Ctl(target_db).write_state(FONTE, "snapshot", "ok" if not failed else "erro", result)
    return result

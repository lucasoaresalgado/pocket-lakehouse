# Novo CAGED (Cadastro Geral de Empregados e Desempregados) — movimentações.
# CAGEDMOV{AAAAMM}.7z from the MTE FTP (ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/{AAAA}/{AAAAMM}/), a ';' text file
# with header. Each row is an aggregated cell (competência, UF, município, CNAE subclass, demographics) with the net
# saldomovimentação — there is no CNPJ at this level. Novo CAGED starts in 2020-01; the legacy format is not handled.
# Table <target_db>.caged_mov: the file's columns (header normalized to ASCII snake_case, values as text) + loaded_at,
# partitioned by competencia_arquivo (the AAAAMM of the file).
# Only the partitions of the competências loaded are replaced, so monthly runs accumulate. One month is ~50 MB of Parquet: the full history (~80 months) does not fit a 5 GiB quota.

import ftplib
import os
import re
import time
import unicodedata
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import core as C

FONTE = "caged"
TABLE = "caged_mov"
FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE = "/pdet/microdados/NOVO CAGED"
PART_COL = "competencia_arquivo"
BYTES_PER_MONTH = 90 << 20  # measured ~77 MB of Parquet for 2026-08 (4.4 M rows), with a margin


def normalize_col(name: str) -> str:
    """NFKD → ASCII (non-ASCII dropped) → lower → non-word runs become '_'"""
    ascii_str = unicodedata.normalize("NFKD", name.strip()).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^\w]+", "_", ascii_str.lower()).strip("_")


def _ftp():
    f = ftplib.FTP(FTP_HOST, timeout=120)
    f.encoding = "latin-1"  # Python 3.8: no encoding= argument
    f.login()
    return f


def ftp_list_years() -> List[str]:
    with _ftp() as f:
        entries = f.nlst(FTP_BASE)
    return sorted(os.path.basename(e) for e in entries if re.fullmatch(r"\d{4}", os.path.basename(e)))


def ftp_list_competencias(year: str) -> List[Tuple[str, str]]:
    """[(AAAAMM, remote path of CAGEDMOV)] — layout {base}/{year}/{AAAAMM}/CAGEDMOV{AAAAMM}.7z"""
    out = []
    with _ftp() as f:
        months = sorted(e for e in f.nlst("%s/%s" % (FTP_BASE, year)) if re.fullmatch(r".*/\d{6}", e))
        for md in months:
            try:
                files = f.nlst(md)
            except ftplib.error_perm:
                continue
            hit = [e for e in files if re.match(r"CAGEDMOV\d{6}", os.path.basename(e), re.I)]
            if hit:
                out.append((os.path.basename(md), hit[0]))
    return sorted(out)


def resolve_competencias(competencia: str) -> List[Tuple[str, str]]:
    """'' → latest; 'AAAA' → every month of the year; 'AAAAMM' → that month"""
    years = ftp_list_years()
    if not years:
        raise ValueError("No year directories found on FTP")
    if re.fullmatch(r"\d{6}", competencia):
        if competencia[:4] not in years:
            raise ValueError("Year %s not on FTP. Available: %s" % (competencia[:4], years))
        pairs = ftp_list_competencias(competencia[:4])
        hit = [p for p in pairs if p[0] == competencia]
        if not hit:
            raise ValueError("Competencia %s not found. Available in %s: %s" % (competencia, competencia[:4], [c for c, _ in pairs]))
        return hit
    if re.fullmatch(r"\d{4}", competencia):
        if competencia not in years:
            raise ValueError("Year %s not on FTP. Available: %s" % (competencia, years))
        pairs = ftp_list_competencias(competencia)
        if not pairs:
            raise ValueError("No CAGEDMOV files found for year %s" % competencia)
        return pairs
    if competencia:
        raise ValueError("competencia must be AAAAMM, AAAA or blank")
    for y in reversed(years):
        pairs = ftp_list_competencias(y)
        if pairs:
            return [pairs[-1]]
    raise ValueError("Could not find any CAGEDMOV file on FTP")


def ftp_download(remote: str, local: str) -> int:
    n = 0
    with _ftp() as f, open(local + ".part", "wb") as out:
        def w(chunk):
            nonlocal n
            out.write(chunk)
            n += len(chunk)
        f.retrbinary("RETR %s" % remote, w, blocksize=8 << 20)
    os.replace(local + ".part", local)
    return n


def extract_txt(archive: str, dest: str) -> str:
    import py7zr
    with py7zr.SevenZipFile(archive, mode="r") as z:
        z.extractall(path=dest)
    txt = [os.path.join(r, f) for r, _, fs in os.walk(dest) for f in fs if f.lower().endswith(".txt")]
    if not txt:
        raise ValueError("No .txt file found in archive %s" % archive)
    return txt[0]


def main(spark: Optional[SparkSession] = None, competencia: str = "", ignorar_cota: bool = False, dry_run: bool = False, target_db: str = "estudo",
         local_files: Optional[List[str]] = None):
    """local_files (tests/offline): [path of CAGEDMOVAAAAMM.7z, ...] instead of the FTP"""
    t0 = time.time()
    competencia = str(competencia or "")
    if local_files:
        jobs = [(re.search(r"(\d{6})", os.path.basename(p)).group(1), p) for p in local_files]
    else:
        jobs = resolve_competencias(competencia)
    plano = {"fonte": FONTE, "competencia_param": competencia or "latest", "competencias": [c for c, _ in jobs],
             "destino": "%s.%s" % (target_db, TABLE), "particao": PART_COL,
             "cota": C.quota_check(len(jobs) * BYTES_PER_MONTH)}
    print("[CAGED] %s" % plano)
    if dry_run:
        return dict(plano, dry_run=True, arquivos=[p for _, p in jobs])
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")

    C.require_space(len(jobs) * BYTES_PER_MONTH, "CAGED %d competência(s)" % len(jobs), ignorar_cota,
                    "Carregue menos meses (competencia=AAAAMM).")
    results, columns = [], None
    C.drop_stage(target_db, TABLE)
    staged = 0
    for comp, remote in jobs:
        t1 = time.time()
        label = "CAGED/%s" % comp
        tmp: List[str] = []
        try:
            xdir = C.raw_dir(FONTE, comp)
            os.makedirs(xdir, exist_ok=True)
            tmp.append(xdir)
            if local_files:
                archive, nbytes = remote, os.path.getsize(remote)
            else:
                archive = os.path.join(xdir, os.path.basename(remote))
                nbytes = ftp_download(remote, archive)
            txt = extract_txt(archive, os.path.join(xdir, "x"))
            if not local_files:
                C.cleanup(archive)
            enc = C.detect_encoding(txt)
            cols = [normalize_col(c) for c in C.split_csv(C.first_line(txt, enc), ";")]
            if columns is None:
                columns = cols
                print("[CAGED] Columns (%d): %s" % (len(cols), cols))
            raw = C.read_csv(spark, txt, ";", enc, header=True)
            df = raw.select(*[C.nz("c%d" % i).alias(c) for i, c in enumerate(cols)])
            df = df.withColumn("loaded_at", F.current_timestamp()).withColumn(PART_COL, F.lit(comp))
            before = staged
            staged = C.stage(df, spark, target_db, TABLE, append=True)
            results.append({"label": label, "success": True, "competencia": comp, "archive_mb": round(nbytes / 1048576, 1),
                            "encoding": enc, "total_rows": staged - before, "duration": "%ds" % (time.time() - t1)})
            print("[CAGED] %s: %.0f MB archive, %d rows" % (label, nbytes / 1048576, staged - before))
        except Exception as e:
            results.append({"label": label, "success": False, "competencia": comp, "error": str(e)[:500], "duration": "%ds" % (time.time() - t1)})
            print("[CAGED] %s FAILED: %s" % (label, e))
        finally:
            C.cleanup(*tmp)
    failed = [r for r in results if not r["success"]]
    touched = C.commit_partitions(spark, target_db, TABLE, PART_COL) if staged else []
    if not staged:
        C.drop_stage(target_db, TABLE)
    result = dict(plano, success=not failed, files_succeeded=len(results) - len(failed), files_failed=len(failed),
                  particoes_gravadas=touched, linhas_carregadas=staged, results=results, duracao_s=round(time.time() - t0),
                  concluido_em=C.utcnow_iso())
    C.Ctl(target_db).write_state(FONTE, "competencia", "ok" if not failed else "parcial", result)
    return result

# Receita Federal — CNPJ open data.
# Zipped CSVs (latin-1, ';', no header) from the Casa dos Dados mirror, one folder per monthly release. Empresas,
# Estabelecimentos and Socios come in 10 parts (0-9); the lookup tables are single files.
# Tables <target_db>.cnpj_{table}, text columns as published (+ loaded_at), each one a SNAPSHOT of the release.
# Notes:
#   - all parts of a table are staged first; the live table is swapped only when every part made it (a failed part
#     keeps the previous snapshot whole);
#   - the full release is ~7 GB zipped and does not fit the 5 GiB bucket quota, so there are study filters:
#     `ufs` keeps the establishments of those states, and Empresas/Socios/Simples are then restricted to the
#     cnpj_basico of those establishments; `parts` loads only some of the 10 parts (a partial snapshot);
#     without `ufs` a size guard refuses a load that would not fit (ignorar_cota=True skips it);
#   - LGPD: the razao_social of sole proprietors/MEIs ends with the owner's open CPF; by default it is stored with the
#     Receita mask (***######**). manter_cpf_aberto=True keeps the source text.

import os
import re
import shutil
import time
import zipfile
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import core as C

FONTE = "rf_cnpj"
BASE_URL = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos"
BROWSER_UA = {"User-Agent": "Mozilla/5.0 (compatible; pocket-lakehouse/1.0)"}

TABLE_COLUMNS: Dict[str, List[str]] = {
    "Empresas": ["cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_do_responsavel", "capital_social",
                 "porte_empresa", "ente_federativo_responsavel"],
    "Estabelecimentos": ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial", "nome_fantasia", "situacao_cadastral",
                         "data_situacao_cadastral", "motivo_situacao_cadastral", "nome_cidade_exterior", "pais", "data_inicio_atividade",
                         "cnae_fiscal_principal", "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero", "complemento",
                         "bairro", "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax",
                         "correio_eletronico", "situacao_especial", "data_situacao_especial"],
    "Socios": ["cnpj_basico", "identificador_socio", "nome_socio_razao_social", "cnpj_cpf_socio", "qualificacao_socio",
               "data_entrada_sociedade", "pais", "representante_legal", "nome_do_representante", "qualificacao_do_representante_legal",
               "faixa_etaria"],
    "Simples": ["cnpj_basico", "opcao_pelo_simples", "data_opcao_pelo_simples", "data_exclusao_do_simples", "opcao_pelo_mei",
                "data_opcao_pelo_mei", "data_exclusao_do_mei"],
    "Cnaes": ["codigo", "descricao"],
    "Municipios": ["codigo", "descricao"],
    "Naturezas": ["codigo", "descricao"],
    "Motivos": ["codigo", "descricao"],
    "Paises": ["codigo", "descricao"],
    "Qualificacoes": ["codigo", "descricao"],
}
PARTITIONED_TABLES = {"Empresas", "Estabelecimentos", "Socios"}
FILTER_BY_BASICO = {"Empresas", "Socios", "Simples"}  # restricted to the selected establishments when `ufs` is used


def sr_table_name(table: str) -> str:
    return "cnpj_%s" % table.lower()


def get_dates(base_url: str = BASE_URL) -> List[str]:
    html = C.http_text(base_url + "/", headers=BROWSER_UA)
    dates = sorted(set(re.findall(r'href="(\d{4}-\d{2}-\d{2})/"', html)))
    if not dates:
        raise ValueError("Could not parse date folders from mirror index")
    return dates


def resolve_date(reference_date: str, base_url: str = BASE_URL) -> str:
    """'AAAA-MM-DD' as is; 'AAAA-MM' → latest release of that month; '' → latest release"""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", reference_date or ""):
        return reference_date
    dates = get_dates(base_url)
    if re.fullmatch(r"\d{4}-\d{2}", reference_date or ""):
        hit = [d for d in dates if d.startswith(reference_date)]
        if not hit:
            raise ValueError("No data for '%s'. Available months: %s" % (reference_date, sorted({d[:7] for d in dates})))
        return hit[-1]
    return dates[-1]


_UNITS = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30}


def file_sizes(release_url: str) -> Dict[str, int]:
    """zip name → approximate size from the Apache index ('537M')"""
    html = C.http_text(release_url + "/", headers=BROWSER_UA)
    out = {}
    for name, num, unit in re.findall(r'href="([^"]+\.zip)".*?<td align="right">\s*([\d.]+)([KMG])\s*</td>', html):
        out[name] = int(float(num) * _UNITS[unit])
    return out


def build_job_list(tables: List[str], parts: Optional[List[int]]) -> List[tuple]:
    jobs = []
    for t in tables:
        if t in PARTITIONED_TABLES:
            jobs += [(t, p) for p in (parts if parts is not None else range(10))]
        else:
            jobs.append((t, None))
    return jobs


def zip_name(table: str, part: Optional[int]) -> str:
    return "%s%s.zip" % (table, "" if part is None else part)


def read_part(spark: SparkSession, zip_path: str, table: str, dest_dir: str, manter_cpf_aberto: bool = False) -> DataFrame:
    """the single CSV of the zip, extracted under a safe name (member names carry '$'), as named string columns"""
    with zipfile.ZipFile(zip_path) as zf:
        name = zf.namelist()[0]
        dest = os.path.join(dest_dir, os.path.basename(zip_path)[:-4] + ".csv")
        os.makedirs(dest_dir, exist_ok=True)
        with zf.open(name) as src, open(dest, "wb") as dst:
            shutil.copyfileobj(src, dst, 8 << 20)
    cols = TABLE_COLUMNS[table]
    raw = C.read_csv(spark, dest, ";", "ISO-8859-1", header=False)
    n_src = len(raw.columns)
    sel = [C.nz("c%d" % i).alias(c) if i < n_src else F.lit(None).cast("string").alias(c) for i, c in enumerate(cols)]
    df = raw.select(*sel).filter(F.col(cols[0]).isNotNull())
    if table == "Empresas" and not manter_cpf_aberto:
        df = df.withColumn("razao_social", mask_trailing_cpf(F.col("razao_social")))
    return df


def mask_trailing_cpf(c):
    """'JOAO DA SILVA 12345678901' → 'JOAO DA SILVA ***456789**'"""
    return F.regexp_replace(c, r"(^|\s)\d{3}(\d{6})\d{2}$", "$1***$2**")


def main(spark: Optional[SparkSession] = None, reference_date: str = "", tables: Optional[List[str]] = None,
         parts: Optional[List[int]] = None, ufs: Optional[List[str]] = None, ignorar_cota: bool = False, manter_cpf_aberto: bool = False,
         dry_run: bool = False, target_db: str = "estudo", base_url: str = BASE_URL):
    t0 = time.time()
    tables = list(tables or TABLE_COLUMNS)
    unknown = [t for t in tables if t not in TABLE_COLUMNS]
    if unknown:
        raise ValueError("Unknown tables: %s. Valid: %s" % (unknown, list(TABLE_COLUMNS)))
    if parts is not None and any(int(p) not in range(10) for p in parts):
        raise ValueError("parts must be within 0..9")
    parts = None if parts is None else sorted({int(p) for p in parts})
    ufs = sorted({str(u).upper() for u in (ufs or [])})
    if "Estabelecimentos" in tables:  # establishments first: the ufs filter of the other tables reads them
        tables = ["Estabelecimentos"] + [t for t in tables if t != "Estabelecimentos"]
    resolved = resolve_date(reference_date, base_url)
    release = "%s/%s" % (base_url, resolved)
    jobs = build_job_list(tables, parts)
    try:
        sizes = file_sizes(release)
    except Exception as e:  # the size guard is a courtesy; a mirror without an index must not block the load
        print("[CNPJ] could not read file sizes: %s" % e)
        sizes = {}
    zip_total = sum(sizes.get(zip_name(t, p), 0) for t, p in jobs)
    replaces = sum(C.table_bytes(target_db, sr_table_name(t)) for t in tables)
    guard = C.quota_check(int(zip_total * 0.9), replaces)  # Parquet+snappy of these CSVs ≈ the zip size
    plano = {"fonte": FONTE, "reference_date": resolved, "tabelas": tables, "parts": parts if parts is not None else "0-9",
             "ufs": ufs or "todas", "arquivos": len(jobs), "zip_total_gib": round(zip_total / (1 << 30), 2),
             "cota": guard, "destino": ["%s.%s" % (target_db, sr_table_name(t)) for t in tables]}
    print("[CNPJ] %s" % plano)
    if dry_run:
        if not ufs and not guard["cabe"]:
            plano["aviso"] = "não cabe na cota sem filtro: use ufs=[...] e/ou parts=[...] (ou aumente a cota)"
        return dict(plano, dry_run=True)
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")
    if not ufs and not guard["cabe"] and not ignorar_cota:
        raise RuntimeError("Carga estimada em %.1f GiB e só há %.1f GiB livres na cota do bucket: use ufs=[...] e/ou parts=[...] "
                           "(ou ignorar_cota=True / QUOTA_GIB maior)" % (guard["estimativa_gib"], guard["livre_gib"]))
    if ufs and any(t in FILTER_BY_BASICO for t in tables) and "Estabelecimentos" not in tables \
            and not C.table_exists(target_db, sr_table_name("Estabelecimentos")):
        raise RuntimeError("ufs filtra Empresas/Socios/Simples pelos estabelecimentos: carregue Estabelecimentos junto ou antes")

    ctl = C.Ctl(target_db)
    results, by_table = [], {}
    for t, p in jobs:
        by_table.setdefault(t, []).append(p)
    for table, tparts in by_table.items():
        sr_table = sr_table_name(table)
        t1 = time.time()
        tmp: List[str] = []
        staged, failed = 0, None
        C.drop_stage(target_db, sr_table)
        basicos = None
        if ufs and table in FILTER_BY_BASICO:
            basicos = (spark.read.parquet(C.uri(C.table_path(target_db, sr_table_name("Estabelecimentos"))))
                       .select("cnpj_basico").distinct())
        for p in tparts:
            label = "%s%s" % (table, "" if p is None else p)
            url = "%s/%s" % (release, zip_name(table, p))
            try:
                print("[CNPJ] %s: downloading %s" % (label, url))
                zpath, nbytes = C.download(url, FONTE, headers=BROWSER_UA)
                tmp.append(zpath)
                xdir = C.raw_dir(FONTE, label)
                tmp.append(xdir)
                df = read_part(spark, zpath, table, xdir, manter_cpf_aberto)
                C.cleanup(zpath)
                if ufs and table == "Estabelecimentos":
                    df = df.filter(F.col("uf").isin(*ufs))
                if basicos is not None:
                    df = df.join(basicos, "cnpj_basico", "left_semi")
                before = staged
                staged = C.stage(df.withColumn("loaded_at", F.current_timestamp()), spark, target_db, sr_table, append=True)
                C.cleanup(xdir)
                print("[CNPJ] %s: %.0f MB zip, %d rows staged (%ds)" % (label, nbytes / 1048576, staged - before, time.time() - t1))
                time.sleep(C.PAUSA_S)
            except Exception as e:
                failed = "%s: %s" % (label, e)
                print("[CNPJ] %s FAILED: %s" % (label, e))
                break
        try:
            if failed:
                raise RuntimeError(failed)
            C.commit(spark, target_db, sr_table, snapshot=True)
            n = spark.read.parquet(C.uri(C.table_path(target_db, sr_table))).count()
            results.append({"label": table, "success": True, "parts": tparts, "sr_rows": n, "duration": "%ds" % (time.time() - t1)})
        except Exception as e:
            C.drop_stage(target_db, sr_table)  # the previous snapshot stays as it was
            results.append({"label": table, "success": False, "error": str(e)[:500], "duration": "%ds" % (time.time() - t1)})
        finally:
            C.cleanup(*tmp)
    failed = [r for r in results if not r["success"]]
    result = dict(plano, success=not failed, files_succeeded=len(results) - len(failed), files_failed=len(failed), results=results,
                  duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
    ctl.write_state(FONTE, "snapshot", "ok" if not failed else "parcial", result)
    print(result)
    return result

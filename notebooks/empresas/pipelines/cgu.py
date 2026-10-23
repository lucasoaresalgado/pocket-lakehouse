# CGU / Portal da Transparência — sanction registries.
# Daily ZIP+CSV snapshots (cp1252, ';', with header) from the CGU open-data CDN:
#   https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/{cadastro}/{AAAAMMDD}_{Nome}.zip
# Only the LATEST snapshot of each registry is online (other dates answer 403); day D appears around 06:45 of D+1.
# Target = today - offset_days (2), probed with HEAD; if it is gone, the nearest available snapshot is used (safe: upsert).
# The CDN sits behind an AWS WAF rate rule that answers HTTP 405 "Human Verification" to bursts: few, spaced probes,
# and a bounded back-off on 405/429.
# Tables (upsert by PK): cgu_ceis, cgu_cnep, cgu_cepim, cgu_acordos_leniencia, cgu_acordos_leniencia_efeitos, cgu_ceaf.
# Every table also has cpf_cnpj_norm / cnpj_basico (or cpf_mascarado_rf for CEAF), data_referencia (last snapshot in
# which the row was seen — filter on it for "the registry as of today") and loaded_at.
# LGPD: CEIS/CNEP publish OPEN CPFs of sanctioned individuals; by default they are stored with
# the Receita mask (***.###.###-** / ***######**, like CEAF), which still joins with cnpj_socios. manter_cpf_aberto=True
# keeps them open.

import csv
import os
import re
import time
import zipfile
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from pyspark.sql import Column, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import core as C

FONTE = "cgu_sancoes"
CDN_BASE = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida"
CSV_ENCODING = "cp1252"  # superset of latin-1: the source uses en-dash (0x96) in headers
MAX_LOOKBACK_DAYS = 10
PROBE_PAUSE_S = 1.5
WAF_RETRY_WAIT_S = 90
WAF_MAX_RETRIES = 4
BROWSER_UA = {"User-Agent": "Mozilla/5.0 (compatible; pocket-lakehouse/1.0)"}

_COLS_SANCAO_COMUM_1 = [
    ("CADASTRO", "cadastro"), ("CÓDIGO DA SANÇÃO", "codigo_da_sancao"), ("TIPO DE PESSOA", "tipo_de_pessoa"),
    ("CPF OU CNPJ DO SANCIONADO", "cpf_ou_cnpj_do_sancionado"), ("NOME DO SANCIONADO", "nome_do_sancionado"),
    ("NOME INFORMADO PELO ÓRGÃO SANCIONADOR", "nome_informado_pelo_orgao_sancionador"),
    ("RAZÃO SOCIAL - CADASTRO RECEITA", "razao_social_cadastro_receita"), ("NOME FANTASIA - CADASTRO RECEITA", "nome_fantasia_cadastro_receita"),
    ("NÚMERO DO PROCESSO", "numero_do_processo"), ("CATEGORIA DA SANÇÃO", "categoria_da_sancao"),
]
_COLS_SANCAO_COMUM_2 = [
    ("DATA INÍCIO SANÇÃO", "data_inicio_sancao"), ("DATA FINAL SANÇÃO", "data_final_sancao"), ("DATA PUBLICAÇÃO", "data_publicacao"),
    ("PUBLICAÇÃO", "publicacao"), ("DETALHAMENTO DO MEIO DE PUBLICAÇÃO", "detalhamento_do_meio_de_publicacao"),
    ("DATA DO TRÂNSITO EM JULGADO", "data_do_transito_em_julgado"),
    ("ABRAGÊNCIA DA SANÇÃO", "abrangencia_da_sancao"),  # sic: the source writes "ABRAGÊNCIA"
    ("ÓRGÃO SANCIONADOR", "orgao_sancionador"), ("UF ÓRGÃO SANCIONADOR", "uf_orgao_sancionador"),
    ("ESFERA ÓRGÃO SANCIONADOR", "esfera_orgao_sancionador"), ("FUNDAMENTAÇÃO LEGAL", "fundamentacao_legal"),
    ("DATA ORIGEM INFORMAÇÃO", "data_origem_informacao"), ("ORIGEM INFORMAÇÕES", "origem_informacoes"), ("OBSERVAÇÕES", "observacoes"),
]

DATASETS: Dict[str, dict] = {
    "ceis": {"zip_name": "CEIS", "csvs": {"CEIS": {
        "table": "cgu_ceis", "pk": ["codigo_da_sancao"], "join_key": "cpf_cnpj_norm", "id_source_col": "cpf_ou_cnpj_do_sancionado",
        "derived": ["cnpj_basico"], "source_columns": _COLS_SANCAO_COMUM_1 + _COLS_SANCAO_COMUM_2}}},
    "cnep": {"zip_name": "CNEP", "csvs": {"CNEP": {
        "table": "cgu_cnep", "pk": ["codigo_da_sancao"], "join_key": "cpf_cnpj_norm", "id_source_col": "cpf_ou_cnpj_do_sancionado",
        "derived": ["cnpj_basico"],
        "source_columns": _COLS_SANCAO_COMUM_1 + [("VALOR DA MULTA", "valor_da_multa")] + _COLS_SANCAO_COMUM_2}}},
    "cepim": {"zip_name": "CEPIM", "csvs": {"CEPIM": {
        "table": "cgu_cepim", "pk": ["cnpj_entidade", "numero_convenio"], "join_key": "cpf_cnpj_norm", "id_source_col": "cnpj_entidade",
        "derived": ["cnpj_basico"],
        "source_columns": [("CNPJ ENTIDADE", "cnpj_entidade"), ("NOME ENTIDADE", "nome_entidade"), ("NÚMERO CONVÊNIO", "numero_convenio"),
                           ("ÓRGÃO CONCEDENTE", "orgao_concedente"), ("MOTIVO DO IMPEDIMENTO", "motivo_do_impedimento")]}}},
    "acordos-leniencia": {"zip_name": "AcordosLeniencia", "csvs": {
        "Acordos": {
            "table": "cgu_acordos_leniencia", "pk": ["id_do_acordo", "cnpj_do_sancionado"], "join_key": "cpf_cnpj_norm",
            "id_source_col": "cnpj_do_sancionado", "derived": ["cnpj_basico"],
            "source_columns": [("ID DO ACORDO", "id_do_acordo"), ("CNPJ DO SANCIONADO", "cnpj_do_sancionado"),
                               ("RAZÃO SOCIAL  CADASTRO RECEITA", "razao_social_cadastro_receita"),
                               ("NOME FANTASIA  CADASTRO RECEITA", "nome_fantasia_cadastro_receita"),
                               ("DATA DE INÍCIO DO ACORDO", "data_de_inicio_do_acordo"), ("DATA DE FIM DO ACORDO", "data_de_fim_do_acordo"),
                               ("SITUAÇÃO DO ACORDO DE LENIÊNICA", "situacao_do_acordo_de_leniencia"),  # sic in the source
                               ("DATA DA INFORMAÇÃO", "data_da_informacao"), ("NÚMERO DO PROCESSO", "numero_do_processo"),
                               ("TERMOS DO ACORDO", "termos_do_acordo"), ("ÓRGÃO SANCIONADOR", "orgao_sancionador")]},
        "Efeitos": {
            "table": "cgu_acordos_leniencia_efeitos", "pk": ["id_do_acordo", "efeito_do_acordo_de_leniencia"], "join_key": None,
            "id_source_col": None, "derived": [],
            "source_columns": [("ID DO ACORDO", "id_do_acordo"), ("EFEITO DO ACORDO DE LENIENCIA", "efeito_do_acordo_de_leniencia"),
                               ("COMPLEMENTO", "complemento")]}}},
    "ceaf": {"zip_name": "CEAF", "csvs": {"Expulsoes": {
        "table": "cgu_ceaf", "pk": ["codigo_da_sancao"], "join_key": "cpf_mascarado_rf", "id_source_col": "cpf_ou_cnpj_do_sancionado",
        "derived": [],
        "source_columns": [("CADASTRO", "cadastro"), ("CÓDIGO DA SANÇÃO", "codigo_da_sancao"), ("TIPO DE PESSOA", "tipo_de_pessoa"),
                           ("CPF OU CNPJ DO SANCIONADO", "cpf_ou_cnpj_do_sancionado"), ("NOME DO SANCIONADO", "nome_do_sancionado"),
                           ("CATEGORIA DA SANÇÃO", "categoria_da_sancao"), ("NÚMERO DO DOCUMENTO", "numero_do_documento"),
                           ("NÚMERO DO PROCESSO", "numero_do_processo"), ("DATA INÍCIO SANÇÃO", "data_inicio_sancao"),
                           ("DATA FINAL SANÇÃO", "data_final_sancao"), ("DATA PUBLICAÇÃO", "data_publicacao"), ("PUBLICAÇÃO", "publicacao"),
                           ("DETALHAMENTO  DO MEIO DE PUBLICAÇÃO", "detalhamento_do_meio_de_publicacao"),
                           ("DATA DO TRÂNSITO EM JULGADO", "data_do_transito_em_julgado"), ("ABRAGÊNCIA DA SANÇÃO", "abrangencia_da_sancao"),
                           ("CARGO EFETIVO", "cargo_efetivo"), ("FUNÇÃO OU CARGO DE CONFIANÇA", "funcao_ou_cargo_de_confianca"),
                           ("ÓRGÃO DE LOTAÇÃO", "orgao_de_lotacao"), ("ÓRGÃO SANCIONADOR", "orgao_sancionador"),
                           ("UF ÓRGÃO SANCIONADOR", "uf_orgao_sancionador"), ("ESFERA ÓRGÃO SANCIONADOR", "esfera_orgao_sancionador"),
                           ("FUNDAMENTAÇÃO LEGAL", "fundamentacao_legal"), ("DATA ORIGEM INFORMAÇÃO", "data_origem_informacao"),
                           ("ORIGEM INFORMAÇÕES", "origem_informacoes"), ("OBSERVAÇÕES", "observacoes")]}}},
}


def norm_header(text: str) -> str:
    """letters/digits only, no accents, upper case, single spaces (the source alternates '-', '–' and double spaces)"""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9]+", " ", C.strip_accents(text))).strip().upper()


def table_columns(spec: dict) -> List[str]:
    """PK, join key, the other source columns (source order), derived columns, data_referencia"""
    cols = list(spec["pk"])
    if spec["join_key"]:
        cols.append(spec["join_key"])
    cols += [c for _, c in spec["source_columns"] if c not in cols]
    return cols + spec["derived"] + ["data_referencia"]


_MASKED = r"^\*{3}\d{6}\*{2}$"


def norm_cpf_cnpj_col(raw: Column) -> Column:
    """'12.800.672/0001-60' → 14 digits; '079.350.991-20' → 11 digits; '***.642.455-**' → '***642455**'; else NULL"""
    s = F.regexp_replace(F.coalesce(raw, F.lit("")), r"[^0-9*]", "")
    return F.when(s.rlike(_MASKED) | (s.rlike(r"^\d+$") & F.length(s).isin(11, 14)), s)


def cnpj_basico_col(n: Column) -> Column:
    return F.when((F.length(n) == 14) & n.rlike(r"^\d+$"), F.substring(n, 1, 8))


def masked_cpf_rf_col(n: Column) -> Column:
    """CEAF publishes '***.642.455-**', the Receita '***642455**'; an open CPF (rare) is masked the same way"""
    return F.when(n.rlike(_MASKED), n).when((F.length(n) == 11) & n.rlike(r"^\d+$"),
                                            F.concat(F.lit("***"), F.substring(n, 4, 6), F.lit("**")))


def zip_url(dataset: str, ref_date: str, base: str = CDN_BASE) -> str:
    return "%s/%s/%s_%s.zip" % (base, dataset, ref_date, DATASETS[dataset]["zip_name"])


def candidate_dates(offset_days: int, today: Optional[date] = None) -> List[str]:
    """target today-offset first; then the newer days up to today (only the latest snapshot stays online); then older ones"""
    today = today or date.today()
    order = list(range(offset_days, -1, -1)) + list(range(offset_days + 1, MAX_LOOKBACK_DAYS + 1))
    return [(today - timedelta(days=b)).strftime("%Y%m%d") for b in order]


def _status(url: str, method: str = "HEAD") -> int:
    try:
        with C.http_open(url, method, headers=BROWSER_UA, timeout=30):
            return 200
    except C.FatalHttp as e:
        return e.code


def resolve_reference_date(dataset: str, offset_days: int, base: str = CDN_BASE, today: Optional[date] = None):
    """(chosen AAAAMMDD, target AAAAMMDD). Few spaced HEAD probes; 405/429 = WAF rate rule → wait and retry the same date."""
    cands = candidate_dates(offset_days, today)
    target, waf_hits, i = cands[0], 0, 0
    while i < len(cands):
        d = cands[i]
        st = _status(zip_url(dataset, d, base))
        if st == 200:
            print("[CGU] %s: snapshot %s — %s" % (dataset, d, "target" if d == target else "fallback (target %s not published)" % target))
            return d, target
        if st in (405, 429):
            waf_hits += 1
            if waf_hits > WAF_MAX_RETRIES:
                raise RuntimeError("[%s] AWS WAF rate rule still active after %d back-offs while probing %s" % (dataset, waf_hits, d))
            print("[WAF] probe %s got HTTP %d; waiting %ds..." % (d, st, WAF_RETRY_WAIT_S * waf_hits))
            time.sleep(WAF_RETRY_WAIT_S * waf_hits)
            continue
        if st not in (403, 404):
            print("[CGU] %s probe %s: HTTP %d" % (dataset, d, st))
        i += 1
        if i < len(cands):
            time.sleep(PROBE_PAUSE_S)
    raise ValueError("[%s] no snapshot found (target %s, lookback %d days)" % (dataset, target, MAX_LOOKBACK_DAYS))


def download_zip(url: str, fonte: str) -> str:
    """download with the WAF back-off; the body must be a ZIP"""
    waf_hits = 0
    while True:
        try:
            path, _ = C.download(url, fonte, headers=BROWSER_UA)
        except C.FatalHttp as e:
            if e.code in (405, 429) and waf_hits < WAF_MAX_RETRIES:
                waf_hits += 1
                print("[WAF] challenge page returned (rate rule); waiting %ds before retry %d..." % (WAF_RETRY_WAIT_S * waf_hits, waf_hits))
                time.sleep(WAF_RETRY_WAIT_S * waf_hits)
                continue
            raise
        with open(path, "rb") as f:
            if f.read(2) != b"PK":
                raise RuntimeError("downloaded body is not a ZIP archive")
        return path


def find_csv_entry(zf: zipfile.ZipFile, csv_name: str) -> str:
    """entries are named {AAAAMMDD}_{csv_name}.csv (e.g. 20260916_Efeitos.csv)"""
    for name in zf.namelist():
        b = os.path.basename(name).lower()
        if b == "%s.csv" % csv_name.lower() or b.endswith("_%s.csv" % csv_name.lower()):
            return name
    raise ValueError("CSV '%s' not found in ZIP; entries: %s" % (csv_name, zf.namelist()))


def validate_header(header: List[str], spec: dict, label: str) -> None:
    expected = [norm_header(h) for h, _ in spec["source_columns"]]
    got = [norm_header(h) for h in header]
    if got != expected:
        diff = ["  col %d: expected %r, got %r" % (i, e, g) for i, (e, g) in enumerate(zip(expected, got)) if e != g]
        if len(got) != len(expected):
            diff.append("  column count: expected %d, got %d" % (len(expected), len(got)))
        raise ValueError("[%s] source header changed — refusing to load:\n%s" % (label, "\n".join(diff)))


def read_header(path: str) -> List[str]:
    with open(path, encoding=CSV_ENCODING, errors="replace", newline="") as f:
        return [c.strip() for c in next(csv.reader(f, delimiter=";"))]


def count_rows(path: str) -> int:
    with open(path, encoding=CSV_ENCODING, errors="replace", newline="") as f:
        r = csv.reader(f, delimiter=";")
        next(r)
        return sum(1 for row in r if any(row))


def transform(spark: SparkSession, path: str, spec: dict, ref_iso: str, manter_cpf_aberto: bool = False):
    """Spark DataFrame in table_columns() order; rows with a NULL PK removed; duplicates of a PK within the snapshot → last wins"""
    raw = C.read_csv(spark, path, ";", CSV_ENCODING, header=True, multiline=True)
    src = [c for _, c in spec["source_columns"]]
    d = raw.select(*[C.nz("c%d" % i).alias(c) for i, c in enumerate(src)], F.monotonically_increasing_id().alias("_ord"))
    d = d.filter(F.concat_ws("", *[F.coalesce(F.col(c), F.lit("")) for c in src]) != "")
    if spec["id_source_col"] and not manter_cpf_aberto:
        d = d.withColumn(spec["id_source_col"], C.mask_cpf_col(spec["id_source_col"]))
    if spec["id_source_col"]:
        n = norm_cpf_cnpj_col(F.col(spec["id_source_col"]))
        if spec["join_key"] == "cpf_cnpj_norm":
            d = d.withColumn("cpf_cnpj_norm", n)
        elif spec["join_key"] == "cpf_mascarado_rf":
            d = d.withColumn("cpf_mascarado_rf", masked_cpf_rf_col(n))
        if "cnpj_basico" in spec["derived"]:
            d = d.withColumn("cnpj_basico", cnpj_basico_col(n))
    d = d.withColumn("data_referencia", F.lit(ref_iso))
    pk_ok = F.lit(True)
    for k in spec["pk"]:
        pk_ok = pk_ok & F.col(k).isNotNull()
    d = C.persist_disk(d)
    total = d.count()
    d = d.filter(pk_ok)
    skipped = total - d.count()
    w = Window.partitionBy(*spec["pk"]).orderBy(F.col("_ord").desc())
    d = d.withColumn("_rn", F.row_number().over(w))
    dups = d.filter("_rn > 1").count()
    d = d.filter("_rn = 1").drop("_rn", "_ord")
    return d.select(*table_columns(spec)).withColumn("loaded_at", F.current_timestamp()), total, skipped, dups


def process_dataset(spark, dataset: str, ref_date: str, target_date: str, dry_run: bool, full_refresh: bool, target_db: str,
                    base: str, manter_cpf_aberto: bool = False) -> dict:
    t0 = time.time()
    url = zip_url(dataset, ref_date, base)
    ref_iso = datetime.strptime(ref_date, "%Y%m%d").strftime("%Y-%m-%d")
    out = {"dataset": dataset, "reference_date": ref_iso, "target_date": datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d"),
           "fallback": ref_date != target_date, "tables": []}
    tmp: List[str] = []
    try:
        zpath = download_zip(url, FONTE)
        tmp.append(zpath)
        xdir = C.raw_dir(FONTE, dataset)
        tmp.append(xdir)
        with zipfile.ZipFile(zpath) as zf:
            entries = {n: find_csv_entry(zf, n) for n in DATASETS[dataset]["csvs"]}
        paths = dict(zip(entries, C.unzip(zpath, xdir, list(entries.values()))))
        for name, spec in DATASETS[dataset]["csvs"].items():  # validate every CSV BEFORE touching any table
            validate_header(read_header(paths[name]), spec, "%s/%s" % (dataset, name))
        for name, spec in DATASETS[dataset]["csvs"].items():
            if dry_run:
                out["tables"].append({"table": spec["table"], "rows_read": count_rows(paths[name]), "sr_rows": None})
                continue
            df, total, skipped, dups = transform(spark, paths[name], spec, ref_iso, manter_cpf_aberto)
            n = C.load(spark, df, target_db, spec["table"], pk=None if full_refresh else spec["pk"], snapshot=full_refresh)
            t = spark.read.parquet(C.uri(C.table_path(target_db, spec["table"])))
            in_snapshot = t.filter(F.col("data_referencia") == ref_iso).count()
            out["tables"].append({"table": spec["table"], "rows_read": total, "rows_skipped_null_pk": skipped,
                                  "duplicadas_no_snapshot": dups, "upserted": n, "sr_rows": t.count(), "sr_rows_in_snapshot": in_snapshot})
            print("[CGU] %s/%s: %d upserted (%d null PK, %d duplicated PK); table now %d rows, %d in snapshot %s"
                  % (dataset, name, n, skipped, dups, out["tables"][-1]["sr_rows"], in_snapshot, ref_iso))
        return dict(out, success=True, duration="%.1fs" % (time.time() - t0))
    except Exception as e:
        print("[CGU] %s failed: %s" % (dataset, e))
        return dict(out, success=False, error=str(e)[:1000], duration="%.1fs" % (time.time() - t0))
    finally:
        C.cleanup(*tmp)


def main(spark: Optional[SparkSession] = None, cadastros: Optional[List[str]] = None, offset_days: int = 2,
         reference_date: str = "", full_refresh: bool = False, manter_cpf_aberto: bool = False, dry_run: bool = False,
         target_db: str = "estudo", base_url: str = CDN_BASE):
    """dry_run downloads and validates the files but needs no Spark and writes nothing"""
    t0 = time.time()
    cadastros = list(cadastros or DATASETS)
    unknown = [c for c in cadastros if c not in DATASETS]
    if unknown:
        raise ValueError("Unknown cadastros: %s. Valid: %s" % (unknown, list(DATASETS)))
    if reference_date and not re.fullmatch(r"\d{8}", reference_date):
        raise ValueError("reference_date must be AAAAMMDD or blank")
    if not 0 <= offset_days <= MAX_LOOKBACK_DAYS:
        raise ValueError("offset_days must be 0..%d" % MAX_LOOKBACK_DAYS)
    if not dry_run and spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")
    print("[CGU] cadastros=%s target=%s full_refresh=%s dry_run=%s → %s.cgu_*"
          % (cadastros, reference_date or "today-%dd (with fallback)" % offset_days, full_refresh, dry_run, target_db))
    results = []
    for i, ds in enumerate(cadastros):
        try:
            ref, target = (reference_date, reference_date) if reference_date else resolve_reference_date(ds, offset_days, base_url)
        except Exception as e:
            results.append({"dataset": ds, "success": False, "error": str(e)[:500]})
            continue
        results.append(process_dataset(spark, ds, ref, target, dry_run, full_refresh, target_db, base_url, manter_cpf_aberto))
        if i < len(cadastros) - 1:
            time.sleep(PROBE_PAUSE_S)  # be polite to the WAF rate rule
    failed = [r for r in results if not r["success"]]
    result = {"fonte": FONTE, "success": not failed, "dry_run": dry_run, "cadastros_succeeded": len(results) - len(failed),
              "cadastros_failed": len(failed), "results": results, "duracao_s": round(time.time() - t0), "concluido_em": C.utcnow_iso()}
    if not dry_run:
        C.Ctl(target_db).write_state(FONTE, "full_refresh" if full_refresh else "upsert", "ok" if not failed else "parcial", result)
    return result

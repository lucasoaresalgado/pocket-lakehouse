# Compras.gov.br / PNCP (Lei 14.133) — contratações, itens e resultados.
# Tables (upsert by PK; reloading overlapping windows is safe):
#   comprasgov_pncp_contratacoes  PK numero_controle_pncp                                       1 row per procurement
#   comprasgov_pncp_itens         PK numero_controle_pncp_compra + numero_item_pncp             1 row per item
#   comprasgov_pncp_resultados    PK + sequencial_resultado (winning supplier CNPJ/CPF, homologated prices)
# mode="api": daily incremental through the public API (dadosabertos.compras.gov.br), pages of 500, window
#   [today - lookback_days, today] by publication / inclusion / result date, sliced to the API's 365-day limit;
#   contratações must be queried per Comprasnet modalidade code (0, 3, 5, 6, 7 — NOT the PNCP ids).
# mode="csv": bulk backfill / re-sync from the repository's monthly
#   zips pncp/{AAAA}/{MM}/{compra|compra_item|item_resultado}_{AAAA}_{MM}.csv.zip (UTF-8, ';', regenerated weekly;
#   partitioned by the procurement's data_publicacao_pncp). csv_months=["2026-08", ...] or csv_years=[2025] select them.
# Columns = union of the CSV columns (lower-cased) and API-only fields; API camelCase is mapped to the CSV snake_case;
# values are text in the CSV format; origem_carga ('api'|'csv'), arquivo_origem and loaded_at are added.
# LGPD: individual suppliers come with OPEN CPFs (resultados.ni_fornecedor,
# itens.cod_fornecedor); by default they are stored with the Receita mask (***######**). manter_cpf_aberto=True keeps them.

import os
import re
import time
from datetime import date, timedelta
from typing import Dict, Generator, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import core as C

FONTE = "comprasgov_pncp"
API_BASE = "https://dadosabertos.compras.gov.br"
CSV_BASE = "https://repositorio.dados.gov.br/seges/comprasgov/pncp"
API_PAGE_SIZE = 500
API_MAX_WINDOW_DAYS = 365
API_PAUSE_S = 0.15
MODALIDADE_CODES = [0, 3, 5, 6, 7]
BROWSER_UA = {"User-Agent": "Mozilla/5.0 (compatible; pocket-lakehouse/1.0)"}

DATASETS: Dict[str, dict] = {
    "contratacoes": {
        "table": "comprasgov_pncp_contratacoes", "csv_file": "compra",
        "api_path": "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
        "api_date_params": ("dataPublicacaoPncpInicial", "dataPublicacaoPncpFinal"), "api_loop_param": "codigoModalidade",
        "pk": ["numero_controle_pncp"],
        "csv_columns": [
            "cod_compra", "modalidade_id_pncp", "modo_disputa_id_pncp", "id_compra", "ind_atual", "valor_total_estimado",
            "valor_total_homologado", "orgao_subrogado_cnpj", "orgao_subrogado_razao_social", "orgao_subrogado_esfera_id",
            "orgao_subrogado_poder_id", "codigo_orgao", "unidade_orgao_uf_nome", "unidade_orgao_codigo_unidade",
            "unidade_orgao_nome_unidade", "unidade_orgao_uf_sigla", "unidade_orgao_municipio_nome", "unidade_orgao_codigo_ibge",
            "unidade_subrogada_uf_nome", "unidade_subrogada_codigo_unidade", "unidade_subrogada_nome_unidade",
            "unidade_subrogada_uf_sigla", "unidade_subrogada_municipio_nome", "unidade_subrogada_codigo_ibge", "numero_compra",
            "data_atualizacao_pncp", "data_publicacao_pncp", "codigo_modo_disputa", "codigo_modalidade", "srp", "ano_compra_pncp",
            "sequencial_compra_pncp", "orgao_entidade_cnpj", "orgao_entidade_razao_social", "orgao_entidade_esfera_id",
            "orgao_entidade_poder_id", "data_inclusao_pncp", "amparo_legal_codigo_pncp", "amparo_legal_nome", "amparo_legal_descricao",
            "data_abertura_proposta_pncp", "data_encerramento_proposta_pncp", "informacao_complementar", "processo", "objeto_compra",
            "link_sistema_origem", "justificativa_presencial", "numero_controle_pncp", "existe_resultado", "modalidade_nome",
            "orcamento_sigiloso_codigo", "orcamento_sigiloso_descricao", "situacao_compra_id_pncp", "situacao_compra_nome_pncp",
            "tipo_instrumento_convocatorio_codigo_pncp", "tipo_instrumento_convocatorio_nome", "modo_disputa_nome_pncp",
            "usuario_nome", "contratacao_excluida"],
        "api_extra_columns": [], "api_overrides": {},
    },
    "itens": {
        "table": "comprasgov_pncp_itens", "csv_file": "compra_item",
        "api_path": "/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133",
        "api_date_params": ("dataInclusaoPncpInicial", "dataInclusaoPncpFinal"), "api_loop_param": None,
        "pk": ["numero_controle_pncp_compra", "numero_item_pncp"],
        "csv_columns": [
            "srk_pncp_item_compra", "cod_compra", "cod_item_compra", "codigo_classe", "codigo_grupo", "numero_item_pncp", "numero_grupo",
            "numero_item_compra", "descricao_detalhada", "descricao_resumida", "material_ou_servico", "material_ou_servico_nome",
            "valor_unitario_estimado", "valor_total", "quantidade", "unidade_medida", "orcamento_sigiloso", "item_categoria_id_pncp",
            "item_categoria_nome", "patrimonio", "codigo_registro_imobiliario", "criterio_julgamento_id_pncp", "criterio_julgamento_nome",
            "situacao_compra_item", "situacao_compra_item_nome", "tipo_beneficio", "tipo_beneficio_nome", "incentivo_produtivo_basico",
            "data_inclusao_pncp", "data_atualizacao_pncp", "tem_resultado", "cod_item_catalogo", "id_contratacao_pncp", "cod_fornecedor",
            "nome_fornecedor", "data_resultado", "valor_total_resultado", "valor_unitario_resultado", "quantidade_resultado", "id_compra",
            "id_compra_item", "ano_compra", "sequencial_compra", "orgao_entidade_cnpj", "unidade_orgao_codigo_unidade",
            "margem_preferencia_normal", "percentual_margem_preferencia_normal", "margem_preferencia_adicional",
            "percentual_margem_preferencia_adicional", "codigo_ncm", "descricao_ncm", "numero_controle_pncp_compra", "txt_link_material",
            "txt_link_classe", "txt_link_servico", "txt_link_grupo", "cod_resultado_item"],
        "api_extra_columns": ["codigo_pdm", "nome_pdm"], "api_overrides": {"descricaodetalhada": "descricao_detalhada"},
        "cpf_columns": ["cod_fornecedor"],
        "csv_aliases": {"descricao": "descricao_resumida"},
        "csv_derive": {"numero_controle_pncp_compra": "id_contratacao_pncp"},  # older files
    },
    "resultados": {
        "table": "comprasgov_pncp_resultados", "csv_file": "item_resultado",
        "api_path": "/modulo-contratacoes/3_consultarResultadoItensContratacoes_PNCP_14133",
        "api_date_params": ("dataResultadoPncpInicial", "dataResultadoPncpFinal"), "api_loop_param": None,
        "pk": ["numero_controle_pncp_compra", "numero_item_pncp", "sequencial_resultado"],
        "csv_columns": [
            "srk_item_resultado", "id_compra", "id_compra_item", "id_contratacao_pncp", "data_atualizacao_pncp", "ni_fornecedor",
            "tipo_pessoa", "data_inclusao_pncp", "numero_item_pncp", "data_cancelamento_pncp", "nome_razao_social_fornecedor",
            "codigo_pais", "porte_fornecedor_id", "quantidade_homologada", "valor_unitario_homologado", "valor_total_homologado",
            "percentual_desconto", "indicador_subcontratacao", "ordem_classificacao_srp", "data_resultado_pncp", "motivo_cancelamento",
            "numero_controle_pncp_compra", "porte_fornecedor_nome", "situacao_compra_item_resultado_nome", "sequencial_resultado",
            "natureza_juridica_nome", "natureza_juridica_id", "situacao_compra_item_resultado_id", "orgao_entidade_cnpj",
            "sequencial_compra", "ano_compra", "unidade_orgao_codigo_unidade", "aplicacao_margem_preferencia",
            "amparo_legal_margem_preferencia_id", "amparo_legal_margem_preferencia_nome", "amparo_legal_margem_preferencia_descricao",
            "aplicacao_beneficio_meepp", "aplicacao_criterio_desempate", "amparo_legal_criterio_desempate_id",
            "amparo_legal_criterio_desempate_nome", "amparo_legal_criterio_desempate_descricao", "moeda_estrangeira_id",
            "data_cotacao_moeda_estrangeira", "valor_nominal_moeda_estrangeira", "pais_origem_produto_servico_id",
            "timezone_cotacao_moeda_estrangeira", "unidade_orgao_uf_sigla", "cod_resultado_item"],
        "api_extra_columns": [], "api_overrides": {}, "cpf_columns": ["ni_fornecedor"],
    },
}
CONTROL_COLUMNS = ["origem_carga", "arquivo_origem"]


def table_columns(spec: dict) -> List[str]:
    cols = list(spec["pk"])
    cols += [c for c in spec["csv_columns"] + spec["api_extra_columns"] if c not in cols]
    return cols + CONTROL_COLUMNS


def camel_to_snake(name: str) -> str:
    """idContratacaoPNCP → id_contratacao_pncp; numeroControlePNCPCompra → numero_controle_pncp_compra"""
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()


def norm_value(v) -> Optional[str]:
    """API/CSV value → text in the CSV format; None/'' → NULL"""
    if v is None:
        return None
    if isinstance(v, bool):
        return "True" if v else "False"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).strip()
    if not s:
        return None
    if len(s) == 19 and s[10] == "T" and s[4] == "-":
        s = s[:10] + " " + s[11:]
    return s


def date_windows(start: date, end: date, max_days: int) -> List[tuple]:
    out, cur = [], start
    while cur <= end:
        stop = min(end, cur + timedelta(days=max_days - 1))
        out.append((cur, stop))
        cur = stop + timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# Source A — API (incremental)
# ---------------------------------------------------------------------------

def api_get(path: str, params: dict, api_base: str = API_BASE) -> dict:
    return C.http_json(api_base + path, params, headers=BROWSER_UA, timeout=180, retries=6)


def api_rows(spec: dict, start: date, end: date, codes: List[int], stats: dict, api_base: str = API_BASE) -> Generator[dict, None, None]:
    """windows <= 365 days x (modalidades, when required) x pages"""
    p_ini, p_fim = spec["api_date_params"]
    colset = set(table_columns(spec))
    unknown = set()
    for w0, w1 in date_windows(start, end, API_MAX_WINDOW_DAYS):
        for code in (codes if spec["api_loop_param"] else [None]):
            page = 1
            while True:
                params = {"pagina": page, "tamanhoPagina": API_PAGE_SIZE, p_ini: w0.isoformat(), p_fim: w1.isoformat()}
                if code is not None:
                    params[spec["api_loop_param"]] = code
                body = api_get(spec["api_path"], params, api_base)
                rows = body.get("resultado") or []
                if page == 1:
                    tag = " %s=%s" % (spec["api_loop_param"], code) if code is not None else ""
                    print("[API] %s %s..%s%s: %s rows / %s pages" % (spec["table"], w0, w1, tag, body.get("totalRegistros", 0),
                                                                     body.get("totalPaginas", 0)))
                for r in rows:
                    rec = {}
                    for k, v in r.items():
                        col = spec["api_overrides"].get(k) or camel_to_snake(k)
                        if col in colset:
                            rec[col] = norm_value(v)
                        elif k not in unknown:
                            unknown.add(k)
                            print("[API] WARNING: unknown field '%s' in %s — ignored" % (k, spec["table"]))
                    rec["origem_carga"] = "api"
                    rec["arquivo_origem"] = spec["api_path"].rsplit("/", 1)[-1]
                    yield rec
                stats["api_requests"] += 1
                if not rows or body.get("paginasRestantes", 0) <= 0:
                    break
                page += 1
                time.sleep(API_PAUSE_S)


# ---------------------------------------------------------------------------
# Source B — monthly CSV zips of the repository (backfill / re-sync)
# ---------------------------------------------------------------------------

def csv_months_for(csv_months: Optional[List[str]], csv_years: Optional[List[int]]) -> List[str]:
    if csv_months:
        bad = [m for m in csv_months if not C.is_ym(str(m))]
        if bad:
            raise ValueError("csv_months inválidos: %s (use AAAA-MM)" % bad)
        return sorted(set(str(m) for m in csv_months))
    if csv_years:
        return [m for y in sorted(set(int(y) for y in csv_years)) for m in C.ym_range("%d-01" % y, "%d-12" % y)]
    raise ValueError("mode=csv exige csv_months=[\"AAAA-MM\", ...] ou csv_years=[AAAA, ...]")


def csv_url(spec: dict, ym: str, base: str = CSV_BASE) -> str:
    y, m = ym.split("-")
    return "%s/%s/%s/%s_%s_%s.csv.zip" % (base, y, m, spec["csv_file"], y, m)


def resolve_csv_header(header: List[str], spec: dict, fname: str) -> List[str]:
    """exact name → ok; name + '_pncp' exists → alias; explicit alias → alias; unknown column → fail; expected but absent → NULL"""
    expected = set(spec["csv_columns"] + spec["api_extra_columns"])
    aliases = spec.get("csv_aliases", {})
    resolved, unknown = [], []
    for h in header:
        if h in expected:
            resolved.append(h)
        elif h in aliases and aliases[h] in expected:
            resolved.append(aliases[h])
        elif "%s_pncp" % h in expected:
            resolved.append("%s_pncp" % h)
        else:
            unknown.append(h)
            resolved.append(h)
    if unknown:
        raise ValueError("[%s] CSV header changed — unknown columns %s; refusing to load" % (fname, unknown))
    if len(set(resolved)) != len(resolved):
        raise ValueError("[%s] CSV header maps to duplicate columns; refusing to load" % fname)
    missing = [c for c in spec["csv_columns"] if c not in resolved]
    if missing:
        print("[CSV] %s: %d expected columns absent (NULL): %s" % (fname, len(missing), missing))
    return resolved


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def finish(df, spec: dict, manter_cpf_aberto: bool = False):
    """table column order, NULL-PK rows dropped, duplicated PKs within the batch → last one wins, open CPFs masked"""
    cols = table_columns(spec)
    for c in cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    if not manter_cpf_aberto:
        for c in spec.get("cpf_columns", []):
            df = df.withColumn(c, C.mask_cpf_col(c))
    pk_ok = F.lit(True)
    for k in spec["pk"]:
        pk_ok = pk_ok & F.col(k).isNotNull()
    w = Window.partitionBy(*spec["pk"]).orderBy(F.col("_ord").desc())
    out = df.filter(pk_ok).withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    return out.select(*cols).withColumn("loaded_at", F.current_timestamp()), df.filter(~pk_ok).count()


def main(spark: Optional[SparkSession] = None, mode: str = "api", datasets: Optional[List[str]] = None, lookback_days: int = 3,
         date_start: str = "", date_end: str = "", csv_months: Optional[List[str]] = None, csv_years: Optional[List[int]] = None,
         modalidade_codes: Optional[List[int]] = None, full_refresh: bool = False, manter_cpf_aberto: bool = False,
         ignorar_cota: bool = False, dry_run: bool = False, target_db: str = "estudo", api_base: str = API_BASE, csv_base: str = CSV_BASE):
    t0 = time.time()
    datasets = list(datasets or DATASETS)
    unknown = [d for d in datasets if d not in DATASETS]
    if unknown:
        raise ValueError("unknown datasets %s; valid: %s" % (unknown, list(DATASETS)))
    if mode not in ("api", "csv"):
        raise ValueError("mode must be 'api' or 'csv'")
    codes = [int(c) for c in (modalidade_codes or [])] or MODALIDADE_CODES
    if mode == "api":
        end = date.fromisoformat(date_end) if date_end else date.today()
        start = date.fromisoformat(date_start) if date_start else end - timedelta(days=lookback_days)
        if start > end:
            raise ValueError("date_start after date_end")
        scope = "API window %s..%s, modalidades %s" % (start, end, codes)
    else:
        months = csv_months_for(csv_months, csv_years)
        scope = "CSV months %s..%s (%d)" % (months[0], months[-1], len(months))
    print("[PNCP] %s | datasets=%s full_refresh=%s dry_run=%s → %s" % (scope, datasets, full_refresh, dry_run, target_db))
    if dry_run:
        check = {}
        if mode == "api":
            for name in datasets:
                spec = DATASETS[name]
                p_ini, p_fim = spec["api_date_params"]
                params = {"pagina": 1, "tamanhoPagina": 10, p_ini: start.isoformat(), p_fim: end.isoformat()}
                if spec["api_loop_param"]:
                    params[spec["api_loop_param"]] = codes[0]
                try:
                    check[name] = {"total_registros": api_get(spec["api_path"], params, api_base).get("totalRegistros")}
                except Exception as e:
                    check[name] = {"erro": str(e)[:200]}
        else:
            for name in datasets:
                u = csv_url(DATASETS[name], months[-1], csv_base)
                try:
                    check[name] = dict(C.head_info(u), mes=months[-1])
                except Exception as e:
                    check[name] = {"url": u, "erro": str(e)[:200]}
                time.sleep(C.PAUSA_S)
        out = {"fonte": FONTE, "mode": mode, "scope": scope, "dry_run": True, "verificacao_fonte": check}
        if mode == "api" and check and all(v.get("total_registros") == 0 for v in check.values()):
            out["aviso"] = ("a API não tem registros nesta janela — o espelho do PNCP atrasa (em 10/2026, ~4 semanas); "
                            "use date_start/date_end mais antigos ou mode=csv")
        return out
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")
    if mode == "csv":  # size guard from the zip sizes (Parquet ≈ the zip size; upserts are guarded again at commit)
        total = 0
        for name in datasets:
            for ym in months:
                try:
                    total += C.head_info(csv_url(DATASETS[name], ym, csv_base)).get("tamanho") or 0
                except C.FatalHttp:
                    pass
        C.require_space(int(total * 1.5), "PNCP CSV %s..%s" % (months[0], months[-1]), ignorar_cota, "Carregue menos meses (csv_months).")

    results, failed = {}, []
    for name in datasets:
        spec = DATASETS[name]
        stats = {"api_requests": 0}
        tmp: List[str] = []
        t1 = time.time()
        try:
            C.drop_stage(target_db, spec["table"])
            if mode == "api":
                cols = table_columns(spec)
                bdir = C.raw_dir(FONTE, name)
                os.makedirs(bdir, exist_ok=True)
                tmp.append(bdir)
                buf, ordn, part = [], 0, 0

                schema = pa.schema([(c, pa.string()) for c in cols] + [("_ord", pa.int64())])

                def flush():  # API pages → local Parquet chunks, so a long window never sits in memory
                    nonlocal buf, part
                    pdf = pd.DataFrame(buf, columns=cols).astype(object)
                    pdf["_ord"] = range(ordn - len(buf), ordn)
                    pq.write_table(pa.Table.from_pandas(pdf, schema=schema, preserve_index=False),
                                   os.path.join(bdir, "part-%05d.parquet" % part))
                    buf, part = [], part + 1

                for rec in api_rows(spec, start, end, codes, stats, api_base):
                    buf.append(rec)
                    ordn += 1
                    if len(buf) >= 100000:
                        flush()
                if buf or part == 0:
                    flush()
                raw = spark.read.schema(", ".join("%s string" % c for c in cols) + ", _ord long").parquet("file://" + bdir)
                df, skipped = finish(raw, spec, manter_cpf_aberto)
                n = C.stage(df, spark, target_db, spec["table"])
                files = [{"api_requests": stats["api_requests"], "rows": n}]
            else:
                n, skipped, files = 0, 0, []
                for ym in months:
                    url = csv_url(spec, ym, csv_base)
                    try:
                        zpath, nbytes = C.download(url, FONTE, headers=BROWSER_UA)
                    except C.FatalHttp as e:
                        if e.code == 404:
                            print("[CSV] %s: not published (404)" % url)
                            continue
                        raise
                    tmp.append(zpath)
                    xdir = C.raw_dir(FONTE, "%s_%s" % (name, ym))
                    tmp.append(xdir)
                    paths = C.unzip(zpath, xdir, None)
                    C.cleanup(zpath)
                    p = [x for x in paths if x.lower().endswith(".csv")][0]
                    fname = os.path.basename(p)
                    header = resolve_csv_header([h.strip().lower() for h in C.split_csv(C.first_line(p, "UTF-8"), ";")], spec, fname)
                    raw = C.read_csv(spark, p, ";", "UTF-8", header=True, multiline=True)
                    d = raw.select(*[C.nz("c%d" % i).alias(h) for i, h in enumerate(header)], F.monotonically_increasing_id().alias("_ord"))
                    for target, source in spec.get("csv_derive", {}).items():
                        if target in d.columns and source in d.columns:
                            d = d.withColumn(target, F.coalesce(F.col(target), F.col(source)))
                        elif source in d.columns:
                            d = d.withColumn(target, F.col(source))
                    d = d.withColumn("origem_carga", F.lit("csv")).withColumn("arquivo_origem", F.lit(fname))
                    df, sk = finish(d, spec, manter_cpf_aberto)
                    before = n
                    n = C.stage(df, spark, target_db, spec["table"], append=True)
                    skipped += sk
                    files.append({"file": fname, "mb": round(nbytes / 1048576, 1), "rows": n - before})
                    print("[CSV] %s: %d rows staged" % (fname, n - before))
                    C.cleanup(xdir)
                    time.sleep(C.PAUSA_S)
            if n:
                C.commit(spark, target_db, spec["table"], pk=None if full_refresh else spec["pk"], snapshot=full_refresh)
            else:
                C.drop_stage(target_db, spec["table"])
            in_table = spark.read.parquet(C.uri(C.table_path(target_db, spec["table"]))).count() \
                if C.table_exists(target_db, spec["table"]) else 0
            results[name] = {"success": True, "rows": n, "table_rows": in_table, "skipped_null_pk": skipped, "files": files,
                             "seconds": round(time.time() - t1, 1)}
            print("[PNCP] %s: %d rows %s, %d skipped (null PK); table now %d" % (name, n, "loaded" if n else "(nothing new)", skipped, in_table))
        except Exception as e:
            C.drop_stage(target_db, spec["table"])
            print("[PNCP] %s FAILED: %s" % (name, e))
            results[name] = {"success": False, "error": str(e)[:500]}
            failed.append(name)
        finally:
            C.cleanup(*tmp)
    result = {"fonte": FONTE, "success": not failed, "mode": mode, "scope": scope, "results": results,
              "duration_s": round(time.time() - t0, 1), "completed_at": C.utcnow_iso()}
    C.Ctl(target_db).write_state(FONTE, mode, "ok" if not failed else "parcial", result)
    return result

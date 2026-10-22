"""ANM — CFEM (mining royalties) collected → <target_db>.anm_cfem_arrecadacao.

Source: ANM open data, one national file (CSV, ';', latin1/utf-8) with the whole history.
Grain: month × process × holder × substance × municipality. Filters the focus (substances/states), aggregates and writes.
LGPD: an individual (PF) holder loses CPF and process number and is aggregated as 'PF'.

Time modes (months as YYYY-MM):
  incremental: window = [min(watermark+1, end-lookback_meses+1), end]; empty table = since 2015-01.
               Skips the download when ETag/Last-Modified did not change (force=True ignores this).
  range:       exactly [start_date, end_date].
  backfill:    [start_date|2015-01, end_date|reference month] in one pass → has_more=False.
  profile:     reads, transforms and returns a profile — writes NOTHING.
  dry_run:     returns only the plan (window, focus, URLs) + a light source check (HEAD + header), no download.
"""
import time
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import common as C

FONTE = "anm_cfem"
TABLE = "anm_cfem_arrecadacao"
FIRST_YM = "2015-01"
URLS_PADRAO = ["https://dadosabertos.anm.gov.br/CFEM/CFEM_Arrecadacao.csv"]

SCHEMA = [("ano_mes", "date"), ("chave", "string"), ("uf", "string"), ("cod_ibge", "string"), ("municipio", "string"),
          ("municipio_norm", "string"), ("substancia_origem", "string"), ("substancia", "string"), ("prioridade", "string"),
          ("tipo_titular", "string"), ("titular_doc", "string"), ("processo", "string"), ("unidade", "string"),
          ("quantidade", "decimal(24,4)"), ("valor_recolhido", "decimal(20,2)"), ("n_registros", "int"), ("loaded_at", "timestamp")]

SPEC = [
    ("anoprocesso", r"^ANODOPROCESSO|^ANOPROCESSO"),
    ("ano", r"^ANO$|^ANOREFERENCIA|^ANOCOMPETENCIA"),
    ("mes", r"^MES$|^MESREFERENCIA|^MESCOMPETENCIA"),
    ("data", r"^DATA|^COMPETENCIA|^PERIODO"),
    ("processo", r"^PROCESSO"),
    ("tipo", r"PFPJ|TIPOPESSOA|^TIPOTITULAR"),
    ("doc", r"CPFCNPJ|^CNPJ|^CPF|DOCUMENTO"),
    ("substancia", r"^SUBSTANCIA"),
    ("uf", r"^UF$|^SIGLAUF|^SIGLAESTADO|^ESTADO$"),
    ("cod_municipio", r"^CODIGOMUNICIPIO|^CODMUNICIPIO|^CODIGOIBGE|^CODIBGE"),
    ("municipio", r"^MUNICIPIO|^NOMEMUNICIPIO"),
    ("quantidade", r"^QUANTIDADE|^QTD"),
    ("unidade", r"^UNIDADE"),
    ("valor", r"^VALOR"),
]


def _faltam(idx) -> List[str]:
    falta = [c for c in ("substancia", "uf", "municipio", "valor") if c not in idx]
    if not ("ano" in idx and "mes" in idx) and "data" not in idx:
        falta.append("ano+mes|data")
    return falta


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", reference_date: str = "", start_date: str = "",
         end_date: str = "", lookback_meses: int = 6, urls: Optional[List[str]] = None, incluir_p3: bool = True,
         force: bool = False, profile: bool = False, dry_run: bool = False, target_db: str = "bronze", cfg_db: str = ""):
    t0 = time.time()
    if mode not in ("incremental", "range", "backfill"):
        raise ValueError("invalid mode: %s" % mode)
    if lookback_meses < 1:
        raise ValueError("lookback_meses must be >= 1")
    ref = C.to_ym("reference_date", reference_date) or C.today_local()[:7]
    s_ym, e_ym = C.to_ym("start_date", start_date), C.to_ym("end_date", end_date)
    fontes = [str(u) for u in (urls or URLS_PADRAO)]

    cfg = C.pick_cfg_db(cfg_db, target_db)
    foco = C.load_foco(cfg, FONTE, incluir_p3)
    ctl = C.Ctl(target_db)

    # ---- window -----------------------------------------------------------
    wm = C.watermark_ym(target_db, TABLE)
    if mode == "range":
        if not s_ym or not e_ym:
            raise ValueError("mode=range requires start_date and end_date")
        ini, fim = s_ym, e_ym
    elif mode == "incremental":
        fim = e_ym or ref
        ini = s_ym or (min(C.add_months(fim, -(lookback_meses - 1)), C.add_months(wm, 1)) if wm else FIRST_YM)
    else:
        ini, fim = s_ym or FIRST_YM, e_ym or ref
    if ini > fim:
        raise ValueError("empty window: %s..%s" % (ini, fim))

    plano = {"fonte": FONTE, "mode": mode, "janela": "%s..%s" % (ini, fim), "watermark": wm, "urls": fontes,
             "destino": "%s.%s" % (target_db, TABLE), "foco": C.plano_foco(foco)}
    print("[CFEM] %s" % plano)
    if dry_run:
        return dict(plano, dry_run=True, has_more=False, verificacao_fonte=[C.check_source(u, SPEC, _faltam) for u in fontes])

    if spark is None:
        raise ValueError("spark is required unless dry_run=True")
    try:
        # ---- change detection ------------------------------------------------
        infos = []
        for u in fontes:
            infos.append(C.head_info(u))
            time.sleep(C.PAUSA_S)
        if mode == "incremental" and not force and not profile and ctl.exists():
            if all(C.unchanged(ctl.read_file_info(FONTE, fi["url"]), fi) for fi in infos):
                res = dict(plano, status="sem_mudanca", linhas_carregadas=0, has_more=False, duracao_s=round(time.time() - t0))
                ctl.write_state(FONTE, mode, fim, ini, fim, "sem_mudanca", res)
                return res

        # ---- read, filter and aggregate (distributed) ---------------------------
        mun = C.load_municipios()
        frames = mun.frames(spark)
        parts, baixados, por_arquivo = [], [], []
        for fi in infos:
            path, nbytes = C.download(fi["url"], FONTE)
            baixados.append(path)
            raw, header, idx, livres, delim = C.csv_source(spark, path, SPEC)
            falta = _faltam(idx)
            if falta:
                raise ValueError("CFEM layout changed: missing %s. Header: %s" % (", ".join(falta), " | ".join(header)))
            print("[CFEM] %s: delimiter '%s', columns %s, unused: %s" % (fi["url"], delim, idx, ", ".join(livres)))
            g = C.col_getter(idx)
            if "ano" in idx and "mes" in idx:
                ym = F.coalesce(C.parse_periodo_col(F.concat(g("ano"), F.lit("-"), F.lpad(g("mes"), 2, "0"))),
                                C.parse_periodo_col(F.concat(g("mes"), F.lit(" "), g("ano"))))
            else:
                ym = C.parse_periodo_col(g("data"))
            sub_orig = C.norm_col(g("substancia"))
            sub = C.classifica_col(foco, sub_orig)
            uf = C.norm_col(g("uf")).substr(1, 2)
            tipo_raw = C.norm_col(g("tipo"))
            doc_dig = F.regexp_replace(g("doc"), r"\D", "")
            d = raw.select(ym.alias("ym"), uf.alias("uf"), sub_orig.alias("sub_orig"), sub.alias("sub"), tipo_raw.alias("tipo_raw"),
                           doc_dig.alias("doc_dig"), F.regexp_replace(g("processo"), r"\s", "").alias("proc0"),
                           F.regexp_replace(g("anoprocesso"), r"\D", "").alias("ap"),
                           C.norm_col(g("municipio")).alias("mun_norm"), F.regexp_replace(g("cod_municipio"), r"\D", "").alias("cod_fonte"),
                           C.norm_col(g("unidade")).substr(1, 30).alias("un"),
                           C.parse_num_col(g("quantidade")).alias("qtd"), C.parse_num_col(g("valor")).alias("val"))
            uf_ok = F.col("uf").isin(*sorted(foco.ufs)) if foco.ufs else F.lit(True)
            no_foco = F.coalesce((F.col("ym") >= ini) & (F.col("ym") <= fim) & F.col("sub").isin(*sorted(foco.substancias)) & uf_ok, F.lit(False))
            d = C.persist_disk(d.withColumn("no_foco", no_foco))
            lidas = d.count()
            por_arquivo.append({"url": fi["url"], "mb": round(nbytes / 1048576), "linhas_lidas": lidas,
                                "linhas_no_foco": d.filter("no_foco").count(), "cabecalho": header, "mapeamento": idx})
            fi["_bytes"], fi["_lidas"] = nbytes, lidas
            parts.append(d.filter("no_foco"))
            time.sleep(C.PAUSA_S)

        d = parts[0]
        for p in parts[1:]:
            d = d.unionByName(p)
        is_pf = (F.col("tipo_raw").startswith("PF") | F.col("tipo_raw").startswith("F") | F.col("tipo_raw").contains("FISICA")
                 | ((F.col("tipo_raw") == "") & (F.length("doc_dig") == 11)))
        d = (d.withColumn("is_pf", is_pf)
             .withColumn("doc", F.when(F.col("is_pf"), F.lit("PF")).otherwise(C.pad14(F.col("doc_dig"))))
             .withColumn("proc", F.when(F.col("is_pf"), F.lit("PF")).when(
                 (F.col("proc0") != "") & (F.col("ap") != "") & ~F.col("proc0").contains("/"), F.concat(F.col("proc0"), F.lit("/"), F.col("ap")))
                 .otherwise(F.col("proc0"))))
        d = C.resolve_municipio(d, frames, "uf", "mun_norm", "cod_fonte")
        agg = (d.groupBy("ym", "uf", "mun_norm", "sub_orig", "proc", "doc", "un")
               .agg(F.first("_cod").alias("cod"), F.first("_mun_nome").alias("munic"), F.first("sub").alias("sub"),
                    F.first("is_pf").alias("is_pf"), F.sum(F.coalesce("qtd", F.lit(0.0))).alias("qtd"),
                    F.sum(F.coalesce("val", F.lit(0.0))).alias("val"), F.count(F.lit(1)).alias("n")))
        out = agg.select(
            F.to_date(F.concat("ym", F.lit("-01"))).alias("ano_mes"),
            C.h32("uf", "mun_norm", "sub_orig", "proc", "doc", "un").alias("chave"),
            "uf", F.col("cod").alias("cod_ibge"), F.col("munic").alias("municipio"), F.col("mun_norm").alias("municipio_norm"),
            F.col("sub_orig").alias("substancia_origem"), F.col("sub").alias("substancia"),
            C.prioridade_col(foco, F.col("sub")).alias("prioridade"),
            F.when(F.col("is_pf"), F.lit("PF")).otherwise(F.lit("PJ")).alias("tipo_titular"),
            F.col("doc").alias("titular_doc"), F.col("proc").alias("processo"), F.col("un").alias("unidade"),
            F.round("qtd", 4).alias("quantidade"), F.round("val", 2).alias("valor_recolhido"), F.col("n").cast("int").alias("n_registros"))
        out = C.conform(out, SCHEMA)

        lidas_tot = sum(a["linhas_lidas"] for a in por_arquivo)
        filtradas_tot = sum(a["linhas_no_foco"] for a in por_arquivo)
        if profile:
            res = dict(plano, profile=True, arquivos=por_arquivo, linhas_lidas=lidas_tot, linhas_no_foco_janela=filtradas_tot)
            n_agg = out.count()
            sem_cod = out.filter(F.col("cod_ibge").isNull())
            res.update(linhas_apos_agregacao=n_agg, linhas_pf_agregadas=out.filter("tipo_titular = 'PF'").count(),
                       reducao_pct=round(1000 * (1 - n_agg / lidas_tot)) / 10 if lidas_tot else None,
                       municipios_sem_codigo=sem_cod.select("uf", "municipio_norm").distinct().count(),
                       amostra=[r.asDict() for r in out.limit(10).collect()], duracao_s=round(time.time() - t0))
            return res

        # ---- write -------------------------------------------------------------
        ctl.ensure()
        n = C.load(spark, out, target_db, TABLE, pk=["ano_mes", "chave"])
        for fi in infos:
            ctl.write_file_info(FONTE, fi, fi.get("_bytes", 0), fi.get("_lidas", 0), n)
        sem = (spark.read.parquet(C.uri(C.table_path(target_db, TABLE))).filter(F.col("cod_ibge").isNull())
               .select("uf", "municipio_norm").distinct().count())
        result = dict(plano, status="ok", linhas_lidas=lidas_tot, linhas_no_foco=filtradas_tot, linhas_carregadas=n,
                      municipios_sem_codigo=sem, cursor=fim, has_more=False, duracao_s=round(time.time() - t0),
                      concluido_em=C.utcnow_iso())
        ctl.write_state(FONTE, mode, fim, ini, fim, "ok", result)
        print(result)
        return result
    except Exception as e:
        if not profile and ctl.exists():
            ctl.write_error(FONTE, mode, e)
        raise
    finally:
        C.cleanup(*locals().get("baixados", []))

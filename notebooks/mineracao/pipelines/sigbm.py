# ANM — SIGBM (national registry of mining dams)

import re
import time
from typing import List, Optional

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from . import common as C

FONTE = "anm_sigbm"
TABLE = "anm_sigbm_barragem"
URLS_PADRAO = ["https://dadosabertos.anm.gov.br/SIGBM/Barragens.csv"]

SCHEMA = [("id_barragem", "string"), ("nome", "string"), ("nome_mina", "string"), ("tipo_empreendedor", "string"),
          ("empreendedor_doc", "string"), ("empreendedor", "string"), ("uf", "string"), ("cod_ibge", "string"), ("municipio", "string"),
          ("latitude", "decimal(10,6)"), ("longitude", "decimal(10,6)"), ("minerio_principal", "string"), ("substancia", "string"),
          ("prioridade", "string"), ("categoria_risco", "string"), ("dano_potencial", "string"), ("nivel_emergencia", "string"),
          ("situacao_operacional", "string"), ("inserido_pnsb", "string"), ("tipo_barragem", "string"), ("metodo_construtivo", "string"),
          ("tipo_alteamento", "string"), ("armazena_cianeto", "string"), ("altura_atual_m", "decimal(12,2)"),
          ("volume_atual_m3", "decimal(20,2)"), ("populacao_jusante", "string"), ("pessoas_afetadas", "string"),
          ("dt_inicio_operacao", "date"), ("dt_finalizacao_dce", "date"), ("atributos", "string"), ("carga_em", "timestamp"),
          ("loaded_at", "timestamp")]

SPEC = [
    ("id", r"^ID$"), ("nome", r"^NOME$"), ("empreendedor", r"^EMPREENDEDOR$"), ("doc", r"^CPFCNPJ$"), ("mina", r"^NOMEDAMINA$"),
    ("uf", r"^UF$"), ("municipio", r"^MUNICIPIO$"), ("lat", r"^LATITUDE$"), ("lon", r"^LONGITUDE$"),
    ("cri", r"^CATEGORIADERISCO"), ("dpa", r"^DANOPOTENCIAL"), ("nivel", r"^NIVELDEEMERGENCIA$"),
    ("situacao", r"^SITUACAOOPERACIONAL$"), ("pnsb", r"^INSERIDONAPNSB$"), ("tipo", r"^TIPODEBARRAGEMDEMINERACAO$"),
    ("metodo", r"^METODOCONSTRUTIVODABARRAGEM$"), ("alteamento", r"^TIPODEALTEAMENTO$"), ("cianeto", r"CIANETO$"),
    ("minerio", r"^MINERIOPRINCIPAL"), ("altura", r"^ALTURAMAXIMAATUAL"), ("volume", r"^VOLUMEATUALDORESERVATORIO"),
    ("populacao", r"^EXISTENCIADEPOPULACAOAJUSANTE$"), ("pessoas", r"^NUMERODEPESSOASPOSSIVELMENTE"),
    ("dt_operacao", r"^DATADEINICIODEOPERACAO$"), ("dt_dce", r"^DATADAFINALIZACAODADCE$"),
]
DESCARTE = r"^RT"  # columns naming a person (technical manager) — never stored


def _faltam(idx) -> List[str]:
    return [c for c in ("id", "uf", "municipio", "minerio") if c not in idx]


def coord_col(c):
    """coordinate in degrees/minutes/seconds ("-09°47'16.100''") or decimal → decimal degrees (6 places)"""
    s = F.regexp_replace(C.s_(c), ",", ".")
    dms = r"^(-?)\s*(\d{1,3})\s*°\s*(\d{1,2})\s*'\s*([\d.]+)"
    deg = (F.regexp_extract(s, dms, 2).cast("double") + F.regexp_extract(s, dms, 3).cast("double") / 60
           + F.regexp_extract(s, dms, 4).cast("double") / 3600)
    signed = F.when(F.regexp_extract(s, dms, 1) == "-", -deg).otherwise(deg)
    dec = F.when(s.rlike(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$") & (F.abs(s.cast("double")) <= 180), s.cast("double"))
    return F.round(F.when(s == "", F.lit(None).cast("double")).when(s.rlike(dms), signed).otherwise(dec), 6)


def data_col(c):
    """DD/MM/YYYY (or YYYY-MM-DD) → date"""
    s = C.s_(c)
    br = r"^(\d{1,2})/(\d{1,2})/(\d{4})"
    iso = r"^(\d{4})-(\d{2})-(\d{2})"
    return (F.when(s.rlike(br), F.to_date(F.concat(F.regexp_extract(s, br, 3), F.lit("-"), F.lpad(F.regexp_extract(s, br, 2), 2, "0"),
                                                   F.lit("-"), F.lpad(F.regexp_extract(s, br, 1), 2, "0"))))
            .when(s.rlike(iso), F.to_date(F.substring(s, 1, 10))))


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", urls: Optional[List[str]] = None,
         incluir_p3: bool = True, force: bool = False, profile: bool = False, dry_run: bool = False,
         target_db: str = "estudo", cfg_db: str = ""):
    t0 = time.time()
    if mode not in ("incremental", "backfill"):
        raise ValueError("invalid mode: %s" % mode)
    fontes = [str(u) for u in (urls or URLS_PADRAO)]
    cfg = C.pick_cfg_db(cfg_db, target_db)
    foco = C.load_foco(cfg, FONTE, incluir_p3)
    ctl = C.Ctl(target_db)
    plano = {"fonte": FONTE, "mode": mode, "urls": fontes, "destino": "%s.%s" % (target_db, TABLE), "foco": C.plano_foco(foco)}
    print("[SIGBM] %s" % plano)
    if dry_run:
        return dict(plano, dry_run=True, has_more=False,
                    verificacao_fonte=[C.check_source(u, SPEC, _faltam, default_delim=",") for u in fontes])
    if spark is None:
        raise ValueError("spark is required unless dry_run=True")

    baixados: List[str] = []
    try:
        infos = []
        for u in fontes:
            infos.append(C.head_info(u))
            time.sleep(C.PAUSA_S)
        if mode == "incremental" and not force and not profile and ctl.exists():
            if all(C.unchanged(ctl.read_file_info(FONTE, fi["url"]), fi) for fi in infos):
                res = dict(plano, status="sem_mudanca", linhas_carregadas=0, has_more=False)
                ctl.write_state(FONTE, mode, None, "", "", "sem_mudanca", res)
                return res

        mun = C.load_municipios()
        by_name, _ = mun.frames(spark)
        parts, por_arquivo = [], []
        for fi in infos:
            path, nbytes = C.download(fi["url"], FONTE)
            baixados.append(path)
            raw, header, idx, _, delim = C.csv_source(spark, path, SPEC, default_delim=",")
            falta = _faltam(idx)
            if falta:
                raise ValueError("SIGBM layout changed: missing %s. Header: %s" % (", ".join(falta), " | ".join(header)))
            usados = set(idx.values())
            extras = [i for i in range(len(header)) if i not in usados and not re.search(DESCARTE, C.compact_norm(header[i]))]
            print("[SIGBM] %s: %d columns, mapped %s" % (fi["url"], len(header), idx))
            g = C.col_getter(idx)
            minerio = g("minerio")
            sub = F.when(minerio != "", C.classifica_col(foco, C.norm_col(minerio)))  # no ore reported: sub = NULL, kept by state
            uf = C.norm_col(g("uf")).substr(1, 2)
            doc_raw = g("doc")
            doc_dig = F.regexp_replace(doc_raw, r"\D", "")
            atrib = F.to_json(F.map_filter(
                F.create_map(*[x for i in extras for x in (F.lit(header[i]), F.when(C.s_("c%d" % i) != "", C.s_("c%d" % i)))]),
                lambda k, v: v.isNotNull())) if extras else F.lit("{}")
            d = raw.select(F.regexp_replace(g("id"), r"\D", "").alias("id"), g("nome").alias("nome"), g("mina").alias("mina"),
                           g("empreendedor").alias("empreendedor"), doc_raw.alias("doc_raw"), doc_dig.alias("doc_dig"), uf.alias("uf"),
                           C.norm_col(g("municipio")).alias("mun_norm"), g("lat").alias("lat"), g("lon").alias("lon"),
                           minerio.alias("minerio"), sub.alias("sub"), g("cri").alias("cri"), g("dpa").alias("dpa"),
                           g("nivel").alias("nivel"), g("situacao").alias("situacao"), g("pnsb").alias("pnsb"), g("tipo").alias("tipo"),
                           g("metodo").alias("metodo"), g("alteamento").alias("alteamento"), g("cianeto").alias("cianeto"),
                           C.parse_num_col(g("altura")).alias("altura"), C.parse_num_col(g("volume")).alias("volume"),
                           g("populacao").alias("populacao"), g("pessoas").alias("pessoas"),
                           data_col(g("dt_operacao")).alias("dt_operacao"), data_col(g("dt_dce")).alias("dt_dce"),
                           F.substring(atrib, 1, 65000).alias("atributos"), F.monotonically_increasing_id().alias("_ord"))
            d = d.filter(F.col("id") != "")
            uf_ok = F.col("uf").isin(*sorted(foco.ufs)) if foco.ufs else F.lit(True)
            sub_ok = F.col("sub").isNull() | F.col("sub").isin(*sorted(foco.substancias))
            lidas_tot = d.count()
            d = C.persist_disk(d.filter(sub_ok & uf_ok))
            # first occurrence of each id wins (later duplicates are skipped)
            d = (d.withColumn("_rn", F.row_number().over(Window.partitionBy("id").orderBy("_ord"))).filter("_rn = 1").drop("_rn"))
            por_arquivo.append({"url": fi["url"], "mb": round(nbytes / 1048576, 1), "linhas_lidas": lidas_tot, "colunas": len(header), "mapeamento": idx})
            fi["_bytes"], fi["_lidas"] = nbytes, lidas_tot
            parts.append(d)
            time.sleep(C.PAUSA_S)

        d = parts[0]
        for p in parts[1:]:
            d = d.unionByName(p)
        is_pf = F.col("doc_raw").contains("*") | (F.length("doc_dig") != 14)
        d = d.join(by_name, (d["uf"] == by_name["b_uf"]) & (d["mun_norm"] == by_name["b_mun"]), "left").drop("b_uf", "b_mun")
        out = d.select(
            F.col("id").alias("id_barragem"), F.substring("nome", 1, 300).alias("nome"), F.substring("mina", 1, 300).alias("nome_mina"),
            F.when(is_pf, "PF").otherwise("PJ").alias("tipo_empreendedor"),
            F.when(is_pf, F.lit("PF")).when(F.col("doc_dig") != "", C.pad14(F.col("doc_dig"))).alias("empreendedor_doc"),
            F.when(~is_pf, F.substring("empreendedor", 1, 300)).alias("empreendedor"),
            "uf", F.col("n_cod").alias("cod_ibge"), F.coalesce("n_nome", "mun_norm").alias("municipio"),
            coord_col(F.col("lat")).alias("latitude"), coord_col(F.col("lon")).alias("longitude"),
            F.substring("minerio", 1, 200).alias("minerio_principal"), F.col("sub").alias("substancia"),
            C.prioridade_col(foco, F.col("sub")).alias("prioridade"), F.col("cri").alias("categoria_risco"),
            F.col("dpa").alias("dano_potencial"), F.col("nivel").alias("nivel_emergencia"), F.col("situacao").alias("situacao_operacional"),
            F.col("pnsb").alias("inserido_pnsb"), F.col("tipo").alias("tipo_barragem"), F.col("metodo").alias("metodo_construtivo"),
            F.col("alteamento").alias("tipo_alteamento"), F.col("cianeto").alias("armazena_cianeto"),
            F.col("altura").alias("altura_atual_m"), F.col("volume").alias("volume_atual_m3"), F.col("populacao").alias("populacao_jusante"),
            F.col("pessoas").alias("pessoas_afetadas"), F.col("dt_operacao").alias("dt_inicio_operacao"),
            F.col("dt_dce").alias("dt_finalizacao_dce"), "atributos", F.current_timestamp().alias("carga_em"))
        out = C.conform(out, SCHEMA)
        lidas = sum(a["linhas_lidas"] for a in por_arquivo)

        if profile:
            return dict(plano, profile=True, arquivos=por_arquivo, linhas_lidas=lidas, barragens_no_foco=out.count(),
                        sem_minerio_informado=out.filter(F.col("substancia").isNull()).count(), pf=out.filter("tipo_empreendedor = 'PF'").count(),
                        sem_coordenada=out.filter(F.col("latitude").isNull() | F.col("longitude").isNull()).count(),
                        municipios_sem_codigo=out.filter(F.col("cod_ibge").isNull()).select("uf", "municipio").distinct().count(),
                        amostra=[r.asDict() for r in out.limit(3).collect()], duracao_s=round(time.time() - t0))

        # ---- write (snapshot: the table is swapped only after the load is complete) ----------
        n = C.stage(out, spark, target_db, TABLE)
        if n == 0:
            raise RuntimeError("No dams in focus — snapshot not written (the current table was kept)")
        C.commit(spark, target_db, TABLE, snapshot=True)
        ctl.ensure()
        for fi in infos:
            ctl.write_file_info(FONTE, fi, fi.get("_bytes", 0), fi.get("_lidas", 0), n)
        t = spark.read.parquet(C.uri(C.table_path(target_db, TABLE)))
        result = dict(plano, status="ok", linhas_lidas=lidas, linhas_carregadas=n,
                      sem_minerio_informado=t.filter(F.col("substancia").isNull()).count(), pf=t.filter("tipo_empreendedor = 'PF'").count(),
                      municipios_sem_codigo=t.filter(F.col("cod_ibge").isNull()).select("uf", "municipio").distinct().count(),
                      sem_coordenada=t.filter(F.col("latitude").isNull() | F.col("longitude").isNull()).count(), has_more=False,
                      duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
        ctl.write_state(FONTE, mode, None, "", "", "ok", result)
        print(result)
        return result
    except Exception as e:
        if not profile and ctl.exists():
            ctl.write_error(FONTE, mode, e)
        raise
    finally:
        C.cleanup(*baixados)

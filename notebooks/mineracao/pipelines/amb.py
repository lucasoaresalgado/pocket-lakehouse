# ANM — AMB (Brazilian Mineral Yearbook): raw and processed production

import re
import time
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import common as C

FONTE = "anm_amb"
TABLE = "anm_amb_producao"
FIRST_YEAR = 2010
URLS_PADRAO = ["https://dadosabertos.anm.gov.br/AMB/Producao_Bruta.csv",
               "https://dadosabertos.anm.gov.br/AMB/Producao_Beneficiada.csv"]

SCHEMA = [("ano", "int"), ("chave", "string"), ("tipo", "string"), ("uf", "string"), ("cod_ibge", "string"), ("municipio", "string"),
          ("substancia_origem", "string"), ("substancia", "string"), ("prioridade", "string"), ("metrica", "string"),
          ("unidade", "string"), ("valor", "decimal(28,4)"), ("atributos", "string"), ("loaded_at", "timestamp")]

SPEC = [("ano", r"^ANO"),
        ("uf", r"^UF$|^SIGLAUF|^SIGLAESTADO|^ESTADO$"),
        ("municipio", r"^MUNICIPIO|^NOMEMUNICIPIO"),
        ("substancia", r"^SUBSTANCIA")]
DESCARTE = r"^CPF|^CNPJ|^NOME|^TITULAR|^RAZAOSOCIAL|^COD|^ID$"


def _faltam(idx) -> List[str]:
    return [c for c in ("ano", "substancia") if c not in idx]


def _tipo(url: str) -> str:
    nu = C.norm(url)
    if "BENEFICIAD" in nu:
        return "BENEFICIADA"
    if "BRUTA" in nu:
        return "BRUTA"
    toks = nu.split(" ")
    return toks[-2] if len(toks) >= 2 else "OUTRO"


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", reference_date: str = "", start_date: str = "",
         end_date: str = "", urls: Optional[List[str]] = None, incluir_p3: bool = True, force: bool = False,
         profile: bool = False, dry_run: bool = False, target_db: str = "estudo", cfg_db: str = ""):
    t0 = time.time()
    if mode not in ("incremental", "range", "backfill"):
        raise ValueError("invalid mode: %s" % mode)
    ref = C.to_ym("reference_date", reference_date) or C.today_local()[:7]
    s_ym, e_ym = C.to_ym("start_date", start_date), C.to_ym("end_date", end_date)
    if mode == "range" and (not s_ym or not e_ym):
        raise ValueError("mode=range requires start_date and end_date")
    ano_ini = FIRST_YEAR if mode == "incremental" else int((s_ym or "%d-01" % FIRST_YEAR)[:4])
    ano_fim = int((e_ym or ref)[:4])
    fontes = [str(u) for u in (urls or URLS_PADRAO)]

    cfg = C.pick_cfg_db(cfg_db, target_db)
    foco = C.load_foco(cfg, FONTE, incluir_p3)
    ctl = C.Ctl(target_db)
    plano = {"fonte": FONTE, "mode": mode, "janela": "%d..%d" % (ano_ini, ano_fim), "urls": fontes,
             "destino": "%s.%s" % (target_db, TABLE), "foco": C.plano_foco(foco)}
    print("[AMB] %s" % plano)
    if dry_run:
        return dict(plano, dry_run=True, has_more=False, verificacao_fonte=[C.check_source(u, SPEC, _faltam) for u in fontes])

    if spark is None:
        raise ValueError("spark is required unless dry_run=True")
    try:
        infos = []
        for u in fontes:
            infos.append(C.head_info(u))
            time.sleep(C.PAUSA_S)
        if mode == "incremental" and not force and not profile and ctl.exists():
            if all(C.unchanged(ctl.read_file_info(FONTE, fi["url"]), fi) for fi in infos):
                res = dict(plano, status="sem_mudanca", linhas_carregadas=0, has_more=False)
                ctl.write_state(FONTE, mode, str(ano_fim), str(ano_ini), str(ano_fim), "sem_mudanca", res)
                return res

        mun = C.load_municipios()
        by_name, _ = mun.frames(spark)
        parts, baixados, por_arquivo = [], [], []
        for fi in infos:
            tipo = _tipo(fi["url"])
            path, nbytes = C.download(fi["url"], FONTE)
            baixados.append(path)
            raw, header, idx, _, delim = C.csv_source(spark, path, SPEC)
            falta = _faltam(idx)
            if falta:
                raise ValueError("AMB layout changed: missing %s. Header: %s" % (", ".join(falta), " | ".join(header)))
            usados = set(idx.values())
            livres = [i for i in range(len(header)) if i not in usados and not re.search(DESCARTE, C.compact_norm(header[i]))]
            print("[AMB] %s: type %s, keys %s, other columns: %s" % (fi["url"], tipo, idx, " | ".join(header[i] for i in livres)))
            g = C.col_getter(idx)

            # long format: every column with a number becomes a (metrica, valor) row; the remaining text goes to `atributos`
            nums = [C.parse_num_col("c%d" % i) for i in livres]
            txt_pairs = []
            for i, n in zip(livres, nums):
                txt_pairs += [F.lit(header[i]), F.when((C.s_("c%d" % i) != "") & n.isNull(), C.s_("c%d" % i))]
            atrib = F.to_json(F.map_filter(F.create_map(*txt_pairs), lambda k, v: v.isNotNull())) if txt_pairs else F.lit("{}")
            metricas = F.array(*[F.when((C.s_("c%d" % i) != "") & n.isNotNull(), F.struct(F.lit(header[i]).alias("metrica"), n.alias("valor")))
                                 for i, n in zip(livres, nums)]) if livres else F.array()
            sub_orig = C.norm_col(g("substancia"))
            uf = C.norm_col(g("uf")).substr(1, 2) if "uf" in idx else F.lit("")
            ano = F.regexp_replace(g("ano"), r"\D", "").substr(1, 4).cast("int")
            d = raw.select(ano.alias("ano"), uf.alias("uf"), C.norm_col(g("municipio")).alias("mun_norm"), sub_orig.alias("sub_orig"),
                           C.classifica_col(foco, sub_orig).alias("sub"),
                           F.when(atrib == "{}", F.lit("")).otherwise(F.substring(atrib, 1, 1000)).alias("atrib"),
                           F.filter(metricas, lambda x: x.isNotNull()).alias("mets"))
            d = d.filter(F.col("ano").isNotNull() & (F.col("ano") != 0))
            uf_ok = (F.col("uf") == "") | F.col("uf").isin(*sorted(foco.ufs)) if foco.ufs else F.lit(True)
            d = C.persist_disk(d.withColumn("no_foco", (F.col("ano") >= ano_ini) & (F.col("ano") <= ano_fim)
                                            & F.col("sub").isin(*sorted(foco.substancias)) & uf_ok))
            lidas = d.count()
            por_arquivo.append({"url": fi["url"], "tipo": tipo, "mb": round(nbytes / 1048576, 1), "linhas_lidas": lidas,
                                "linhas_no_foco": d.filter("no_foco").count(), "cabecalho": header, "chaves": idx})
            fi["_bytes"], fi["_lidas"] = nbytes, lidas
            long = (d.filter("no_foco").select("ano", "uf", "mun_norm", "sub_orig", "sub", "atrib", F.explode("mets").alias("m"))
                    .select("ano", F.lit(tipo).alias("tipo"), "uf", "mun_norm", "sub_orig", "sub", "atrib",
                            F.col("m.metrica").alias("metrica"), F.col("m.valor").alias("valor")))
            parts.append(long)
            time.sleep(C.PAUSA_S)

        long = parts[0]
        for p in parts[1:]:
            long = long.unionByName(p)
        agg = (long.groupBy("ano", "tipo", "uf", "mun_norm", "sub_orig", "metrica", "atrib")
               .agg(F.first("sub").alias("sub"), F.sum("valor").alias("val")))
        agg = (agg.join(by_name, (agg["uf"] == by_name["b_uf"]) & (agg["mun_norm"] == by_name["b_mun"]), "left")
               .drop("b_uf", "b_mun"))
        out = agg.select(
            "ano", C.h32("ano", "tipo", "uf", "mun_norm", "sub_orig", "metrica", "atrib").alias("chave"), "tipo", "uf",
            F.col("n_cod").alias("cod_ibge"), F.coalesce("n_nome", "mun_norm").alias("municipio"),
            F.col("sub_orig").alias("substancia_origem"), F.col("sub").alias("substancia"),
            C.prioridade_col(foco, F.col("sub")).alias("prioridade"), F.substring("metrica", 1, 200).alias("metrica"),
            F.substring(F.regexp_extract("metrica", r"\(([^)]*)\)", 1), 1, 60).alias("unidade"),
            F.round("val", 4).alias("valor"), F.col("atrib").alias("atributos"))
        out = C.conform(out, SCHEMA)

        lidas_tot = sum(a["linhas_lidas"] for a in por_arquivo)
        if profile:
            return dict(plano, profile=True, arquivos=por_arquivo, linhas_lidas=lidas_tot, linhas_longas_no_foco=out.count(),
                        metricas=[r.asDict() for r in out.groupBy("metrica").count().orderBy(F.desc("count")).limit(60).collect()],
                        amostra=[r.asDict() for r in out.limit(10).collect()], duracao_s=round(time.time() - t0))

        ctl.ensure()
        n = C.load(spark, out, target_db, TABLE, pk=["ano", "chave"])
        for fi in infos:
            ctl.write_file_info(FONTE, fi, fi.get("_bytes", 0), fi.get("_lidas", 0), n)
        result = dict(plano, status="ok", linhas_lidas=lidas_tot, linhas_carregadas=n,
                      arquivos=[{"url": a["url"], "linhas_lidas": a["linhas_lidas"], "linhas_no_foco": a["linhas_no_foco"]} for a in por_arquivo],
                      cursor=str(ano_fim), has_more=False, duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
        ctl.write_state(FONTE, mode, str(ano_fim), str(ano_ini), str(ano_fim), "ok", result)
        print(result)
        return result
    except Exception as e:
        if not profile and ctl.exists():
            ctl.write_error(FONTE, mode, e)
        raise
    finally:
        C.cleanup(*locals().get("baixados", []))

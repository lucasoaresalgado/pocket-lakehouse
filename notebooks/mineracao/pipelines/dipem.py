# ANM — DIPEM (declared investment in mineral research)

import re
import time
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import common as C

FONTE = "anm_dipem"
TABLE = "anm_dipem_investimento"
URLS_PADRAO = ["https://dadosabertos.anm.gov.br/DIPEM/InvestimentoPesquisaMineralMunicipio.csv"]

SCHEMA = [("ano", "int"), ("chave", "string"), ("uf", "string"), ("cod_ibge", "string"), ("municipio", "string"),
          ("substancia_origem", "string"), ("substancia", "string"), ("prioridade", "string"), ("tipo_investimento", "string"),
          ("valor_rs", "decimal(20,2)"), ("carga_em", "timestamp"), ("loaded_at", "timestamp")]

SPEC = [("cod_municipio", r"^CODIGOMUNICIPIO$|^CODMUNICIPIO$"), ("municipio", r"^MUNICIPIO$"), ("uf", r"^UF$"),
        ("substancia", r"^SUBSTANCIA$"), ("ano", r"^ANO$")]


def tipo_investimento(h: str) -> str:
    """"Valor<Tipo>" → TIPO em snake (ValorAnaliseQuimica → ANALISE_QUIMICA)"""
    return re.sub(r"([a-z])([A-Z])", r"\1_\2", re.sub(r"^Valor", "", h, flags=re.I)).upper()


def _faltam(idx) -> List[str]:
    return [c for c in ("ano", "uf", "substancia") if c not in idx]


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", urls: Optional[List[str]] = None,
         incluir_p3: bool = True, force: bool = False, profile: bool = False, dry_run: bool = False,
         target_db: str = "bronze", cfg_db: str = ""):
    t0 = time.time()
    if mode not in ("incremental", "backfill"):
        raise ValueError("invalid mode: %s" % mode)
    fontes = [str(u) for u in (urls or URLS_PADRAO)]
    cfg = C.pick_cfg_db(cfg_db, target_db)
    foco = C.load_foco(cfg, FONTE, incluir_p3)
    ctl = C.Ctl(target_db)
    plano = {"fonte": FONTE, "mode": mode, "urls": fontes, "destino": "%s.%s" % (target_db, TABLE), "foco": C.plano_foco(foco)}
    print("[DIPEM] %s" % plano)

    if dry_run:
        checks = []
        for u in fontes:
            chk = C.check_source(u, SPEC, _faltam)
            # the Valor* columns are what makes the file loadable; list them from the same header
            try:
                hdr = C.split_csv(C.peek_header(u), chk.get("delimitador", ";"))
                vals = [h for h in hdr if re.match(r"^VALOR", C.compact_norm(h))]
                chk["colunas_valor"] = vals
                if not vals:
                    chk.setdefault("faltam", []).append("Valor*")
                    chk["layout_ok"] = False
            except Exception:
                pass
            checks.append(chk)
        return dict(plano, dry_run=True, has_more=False, verificacao_fonte=checks)
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
        frames = mun.frames(spark)
        parts, por_arquivo = [], []
        for fi in infos:
            path, nbytes = C.download(fi["url"], FONTE)
            baixados.append(path)
            raw, header, idx, _, delim = C.csv_source(spark, path, SPEC)
            val_idx = [i for i, h in enumerate(header) if re.match(r"^VALOR", C.compact_norm(h))]
            falta = _faltam(idx) + ([] if val_idx else ["Valor*"])
            if falta:
                raise ValueError("DIPEM layout changed: missing %s. Header: %s" % (", ".join(falta), " | ".join(header)))
            print("[DIPEM] %s: keys %s, values: %s" % (fi["url"], idx, ", ".join(header[i] for i in val_idx)))
            g = C.col_getter(idx)

            def valor(i):  # the source publishes ",00" for zero
                raw_i = C.s_("c%d" % i)
                return C.parse_num_col(F.when(raw_i.startswith(","), F.concat(F.lit("0"), raw_i)).otherwise(raw_i))

            vals = F.array(*[F.when(valor(i).isNotNull() & (valor(i) != 0), F.struct(F.lit(tipo_investimento(header[i])).alias("tipo"), valor(i).alias("v")))
                             for i in val_idx])
            sub_raw, uf_raw = g("substancia"), g("uf")
            sub_orig = C.norm_col(sub_raw)
            d = raw.select(F.regexp_replace(g("ano"), r"\D", "").substr(1, 4).cast("int").alias("ano"),
                           F.filter(vals, lambda x: x.isNotNull()).alias("vals"), sub_raw.alias("sub_raw"), uf_raw.alias("uf_raw"),
                           C.norm_col(uf_raw).substr(1, 2).alias("uf"), sub_orig.alias("sub_orig"),
                           C.classifica_col(foco, sub_orig).alias("sub"), C.norm_col(g("municipio")).alias("mun_norm"),
                           F.regexp_replace(g("cod_municipio"), r"\D", "").alias("cod_fonte"))
            d = d.filter(F.col("ano").isNotNull() & (F.col("ano") != 0))
            lidas = d.count()
            sem_local = F.col("sub_raw").isin("", "-") | F.col("uf_raw").isin("", "-")
            uf_ok = F.col("uf").isin(*sorted(foco.ufs)) if foco.ufs else F.lit(True)
            foco_rows = d.filter(~sem_local & F.col("sub").isin(*sorted(foco.substancias)) & uf_ok)
            por_arquivo.append({"url": fi["url"], "mb": round(nbytes / 1048576, 1), "linhas_lidas": lidas,
                                "linhas_no_foco": foco_rows.count(), "cabecalho": header})
            fi["_bytes"], fi["_lidas"] = nbytes, lidas
            parts.append(foco_rows.select("ano", "uf", "sub", "sub_orig", "mun_norm", "cod_fonte", F.explode("vals").alias("m")))
            time.sleep(C.PAUSA_S)

        d = parts[0]
        for p in parts[1:]:
            d = d.unionByName(p)
        d = C.resolve_municipio(d, frames, "uf", "mun_norm", "cod_fonte")
        d = d.withColumn("k_mun", F.coalesce("_cod", "mun_norm"))
        agg = (d.groupBy("ano", "uf", "k_mun", "sub_orig", F.col("m.tipo").alias("tipo"))
               .agg(F.first("_cod").alias("cod"), F.first("_mun_nome").alias("munic"), F.first("sub").alias("sub"),
                    F.sum("m.v").alias("val")))
        out = agg.select("ano", C.h32("ano", "uf", "k_mun", "sub_orig", "tipo").alias("chave"), "uf", F.col("cod").alias("cod_ibge"),
                         F.col("munic").alias("municipio"), F.col("sub_orig").alias("substancia_origem"), F.col("sub").alias("substancia"),
                         C.prioridade_col(foco, F.col("sub")).alias("prioridade"), F.col("tipo").alias("tipo_investimento"),
                         F.round("val", 2).alias("valor_rs"), F.current_timestamp().alias("carga_em"))
        out = C.conform(out, SCHEMA)
        lidas_tot = sum(a["linhas_lidas"] for a in por_arquivo)

        if profile:
            return dict(plano, profile=True, arquivos=por_arquivo, linhas_lidas=lidas_tot, linhas_longas_no_foco=out.count(),
                        valor_foco_rs=out.agg(F.sum("valor_rs")).collect()[0][0],
                        municipios_sem_codigo=out.filter(F.col("cod_ibge").isNull()).select("uf", "municipio").distinct().count(),
                        amostra=[r.asDict() for r in out.limit(5).collect()], duracao_s=round(time.time() - t0))

        # ---- write (snapshot: the table is swapped only after the load is complete) ----------
        n = C.stage(out, spark, target_db, TABLE)
        if n == 0:
            raise RuntimeError("No rows in focus — snapshot not written (the current table was kept)")
        C.commit(spark, target_db, TABLE, snapshot=True)
        ctl.ensure()
        for fi in infos:
            ctl.write_file_info(FONTE, fi, fi.get("_bytes", 0), fi.get("_lidas", 0), n)
        t = spark.read.parquet(C.uri(C.table_path(target_db, TABLE)))
        result = dict(plano, status="ok", linhas_lidas=lidas_tot, linhas_carregadas=n, valor_foco_rs=t.agg(F.sum("valor_rs")).collect()[0][0],
                      municipios_sem_codigo=t.filter(F.col("cod_ibge").isNull()).select("uf", "municipio").distinct().count(),
                      has_more=False, duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
        ctl.write_state(FONTE, mode, None, "", "", "ok", result)
        print(result)
        return result
    except Exception as e:
        if not profile and ctl.exists():
            ctl.write_error(FONTE, mode, e)
        raise
    finally:
        C.cleanup(*baixados)

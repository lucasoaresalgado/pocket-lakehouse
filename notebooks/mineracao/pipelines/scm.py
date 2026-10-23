# ANM — Mining Registry (SCM)

import os
import re
import struct
import time
import zipfile
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import common as C

FONTE = "anm_scm"
URLS_PADRAO = ["https://dadosabertos.anm.gov.br/SCM/microdados/microdados-scm.zip"]
RELACOES_CITADAS_DOU = ("1", "2", "10")  # TipoRelacao values whose person is named in the DOU text: holder/applicant, lessee, assignee
T = {"processo": "anm_scm_processo", "substancia": "anm_scm_substancia", "municipio": "anm_scm_municipio",
     "pessoa": "anm_scm_pessoa", "titulo": "anm_scm_titulo", "evento": "anm_scm_evento"}
PK = {"processo": ["processo"], "substancia": ["processo", "chave"], "municipio": ["processo", "cod_ibge"],
      "pessoa": ["processo", "chave"], "titulo": ["processo", "chave"], "evento": ["processo", "chave"]}

_TAIL = [("carga_em", "timestamp"), ("loaded_at", "timestamp")]
SCHEMA = {
    "processo": [("processo", "string"), ("processo_chave", "string"), ("nr_processo", "int"), ("ano_processo", "int"), ("nup", "string"),
                 ("ativo", "boolean"), ("tipo_requerimento", "string"), ("fase", "string"), ("unidade_regional", "string"),
                 ("dt_protocolo", "date"), ("dt_prioridade", "date"), ("area_ha", "decimal(16,2)"), ("substancias_foco", "string"),
                 ("ufs", "string")] + _TAIL,
    "substancia": [("processo", "string"), ("chave", "string"), ("processo_chave", "string"), ("id_substancia", "int"),
                   ("substancia_origem", "string"), ("substancia", "string"), ("prioridade", "string"), ("tipo_uso", "string"),
                   ("motivo_encerramento", "string"), ("dt_inicio", "date"), ("dt_fim", "date")] + _TAIL,
    "municipio": [("processo", "string"), ("cod_ibge", "string"), ("processo_chave", "string"), ("uf", "string"),
                  ("municipio", "string")] + _TAIL,
    "pessoa": [("processo", "string"), ("chave", "string"), ("processo_chave", "string"), ("tipo_relacao", "string"),
               ("tipo_pessoa", "string"), ("documento", "string"), ("nome", "string"), ("n_pessoas", "int"), ("dt_inicio", "date"),
               ("dt_fim", "date")] + _TAIL,
    "titulo": [("processo", "string"), ("chave", "string"), ("processo_chave", "string"), ("nr_titulo", "string"),
               ("documento_legal", "string"), ("tipo_documento_legal", "string"), ("situacao", "string"), ("dt_publicacao", "date"),
               ("dt_vencimento", "date")] + _TAIL,
    "evento": [("processo", "string"), ("chave", "string"), ("processo_chave", "string"), ("dt_evento", "date"), ("id_evento", "int"),
               ("evento", "string"), ("ob_evento", "string"), ("publicacao_dou", "string")] + _TAIL,
}

TABELAS_ZIP = ["Processo", "ProcessoSubstancia", "ProcessoMunicipio", "ProcessoPessoa", "ProcessoTitulo", "ProcessoEvento", "Pessoa",
               "Municipio", "Substancia", "Evento", "FaseProcesso", "TipoRequerimento", "UnidadeAdministrativaRegional",
               "TipoUsoSubstancia", "MotivoEncerramentoSubstancia", "DocumentoLegal", "TipoDocumentoLegal",
               "SituacaoDocumentoLegal", "TipoRelacao"]


# ---------------------------------------------------------------------------
# Remote ZIP (dry run): reads only the central directory with one Range request
# ---------------------------------------------------------------------------

def zip_listing_remoto(url: str, tamanho: int) -> List[str]:
    tail_len = min(tamanho, 1 << 20)
    with C.http_open(url, "GET", {"Range": "bytes=%d-%d" % (tamanho - tail_len, tamanho - 1)}) as r:
        tail = r.read()
    e = tail.rfind(b"PK\x05\x06")
    if e < 0:
        raise ValueError("Invalid ZIP: end of central directory not found")
    n, cd_size, cd_off = struct.unpack("<HII", tail[e + 10:e + 20])
    ini = cd_off - (tamanho - len(tail))
    if ini < 0:
        raise ValueError("central directory outside the bytes read (%d bytes)" % cd_size)
    nomes, p = [], ini
    for _ in range(n):
        fl, el, cl = struct.unpack("<HHH", tail[p + 28:p + 34])
        nomes.append(tail[p + 46:p + 46 + fl].decode("utf-8", "replace").split("/")[-1])
        p += 46 + fl + el + cl
    return nomes


def _extrai(zip_path: str, out_dir: str) -> Dict[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    achados: Dict[str, str] = {}
    with zipfile.ZipFile(zip_path) as z:
        por_nome = {os.path.basename(i.filename): i for i in z.infolist()}
        for t in TABELAS_ZIP:
            info = por_nome.get(t + ".txt")
            if info is None:
                raise ValueError("SCM layout changed: %s.txt is not in the ZIP (%s)" % (t, ", ".join(sorted(por_nome))))
            dest = os.path.join(out_dir, t + ".txt")
            with z.open(info) as src, open(dest, "wb") as dst:
                while True:
                    chunk = src.read(1 << 20)
                    if not chunk:
                        break
                    dst.write(chunk)
            achados[t] = dest
    return achados


# ---------------------------------------------------------------------------
# Reading the ZIP tables, and column helpers
# ---------------------------------------------------------------------------

def tabela(spark: SparkSession, path: str) -> DataFrame:
    """DataFrame of a ZIP table keeping the header names (DSProcesso, IDSubstancia, ...)"""
    lines = [ln for ln in C.head_lines(path, 3) if ln.strip()]
    names = [h.strip() for h in C.split_csv(lines[0], ";")]
    df = C.read_csv(spark, path, ";", C.detect_encoding(path))
    return df.toDF(*names[:len(df.columns)])


def dominio(df: DataFrame) -> DataFrame:
    """domain table ID → description (1st and 2nd columns)"""
    a, b = df.columns[0], df.columns[1]
    return F.broadcast(df.select(F.col(a).alias("d_id"), F.col(b).alias("d_desc")).dropDuplicates(["d_id"]))


def look(df: DataFrame, key: str, dom: DataFrame, out: str) -> DataFrame:
    """left join with a domain table: adds column `out` = description of df[key]"""
    if key not in df.columns:
        return df.withColumn(out, F.lit(None).cast("string"))
    return df.join(dom, df[key] == dom["d_id"], "left").withColumnRenamed("d_desc", out).drop("d_id")


def exige(df: DataFrame, nome: str, cols: List[str]) -> None:
    falta = [c for c in cols if c not in df.columns]
    if falta:
        raise ValueError("SCM layout changed: %s without %s (columns: %s)" % (nome, ", ".join(falta), ", ".join(df.columns)))


def col(df: DataFrame, name: str):
    """column as trimmed string, or NULL when the (optional) column is not in this edition of the file"""
    return C.s_(name) if name in df.columns else F.lit(None).cast("string")


def chave_processo(ds):
    """"812.827/1976" → "812827/1976" (number without dots or leading zeros + year)"""
    parts = F.split(ds, "/")
    num = F.coalesce(F.regexp_replace(parts[0], r"\D", "").cast("long"), F.lit(0))
    return F.when(parts[1].isNotNull() & (parts[1] != ""), F.concat(num.cast("string"), F.lit("/"), F.trim(parts[1])))


def int_or_null(c):
    v = c.cast("int")
    return F.when(v != 0, v)


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", urls: Optional[List[str]] = None,
         incluir_p3: bool = True, force: bool = False, profile: bool = False, dry_run: bool = False,
         target_db: str = "estudo", cfg_db: str = ""):
    t0 = time.time()
    if mode not in ("incremental", "backfill"):
        raise ValueError("invalid mode: %s" % mode)
    fonte = str((urls or URLS_PADRAO)[0])
    cfg = C.pick_cfg_db(cfg_db, target_db)
    foco = C.load_foco(cfg, FONTE, incluir_p3)
    ctl = C.Ctl(target_db)
    plano = {"fonte": FONTE, "mode": mode, "url": fonte, "destino": ["%s.%s" % (target_db, t) for t in T.values()],
             "foco": C.plano_foco(foco)}
    print("[SCM] %s" % plano)
    if dry_run:
        chk = {"url": fonte}
        try:
            fi = C.head_info(fonte)
            chk.update(etag=fi["etag"], last_modified=fi["last_modified"], tamanho=fi["tamanho"])
            if fi["tamanho"]:
                time.sleep(C.PAUSA_S)
                nomes = zip_listing_remoto(fonte, fi["tamanho"])
                faltam = [t + ".txt" for t in TABELAS_ZIP if t + ".txt" not in nomes]
                chk.update(tabelas_no_zip=len(nomes), faltam=faltam, layout_ok=not faltam)
        except Exception as e:
            chk["erro"] = str(e)[:300]
        return dict(plano, dry_run=True, has_more=False, verificacao_fonte=[chk])
    if spark is None:
        raise ValueError("spark is required unless dry_run=True")

    work = os.path.join(C.RAW_DIR, FONTE)
    try:
        fi = C.head_info(fonte)
        time.sleep(C.PAUSA_S)
        if mode == "incremental" and not force and not profile and ctl.exists() and C.unchanged(ctl.read_file_info(FONTE, fi["url"]), fi):
            res = dict(plano, status="sem_mudanca", linhas_carregadas=0, has_more=False)
            ctl.write_state(FONTE, mode, None, "", "", "sem_mudanca", res)
            return res

        def tl(s):
            print("[SCM] %s (%ds)" % (s, round(time.time() - t0)))

        # ---- download the ZIP (~175 MB) to disk and extract the tables --------------------------
        zip_path, zip_bytes = C.download(fonte, FONTE)
        arquivos = _extrai(zip_path, os.path.join(work, "tabelas"))
        tl("ZIP %d MB on disk, %d tables extracted" % (round(zip_bytes / 1048576), len(arquivos)))
        tb = {t: tabela(spark, p) for t, p in arquivos.items()}
        dom = {t: dominio(tb[t]) for t in ("Substancia", "Evento", "FaseProcesso", "TipoRequerimento", "UnidadeAdministrativaRegional",
                                           "TipoUsoSubstancia", "MotivoEncerramentoSubstancia", "DocumentoLegal", "TipoDocumentoLegal",
                                           "SituacaoDocumentoLegal", "TipoRelacao")}
        exige(tb["Municipio"], "Municipio.txt", ["IDMunicipio", "NMMunicipio", "SGUF"])
        mun_fonte = tb["Municipio"].select("IDMunicipio", F.col("NMMunicipio").alias("nm_fonte"), F.col("SGUF").alias("uf_fonte"))
        _, by_cod = C.load_municipios().frames(spark)

        # ZIP substance → standard substance
        sub_std = F.broadcast(dom["Substancia"].select(
            F.col("d_id").alias("s_id"), C.norm_col("d_desc").alias("s_norm"), C.classifica_col(foco, C.norm_col("d_desc")).alias("s_std")))

        # ---- 1) processes with a focus substance (process → "FERRO,OURO") ---------------------
        ps = tb["ProcessoSubstancia"].join(sub_std, tb["ProcessoSubstancia"]["IDSubstancia"] == sub_std["s_id"], "left")
        cand = (ps.filter(F.col("s_std").isin(*sorted(foco.substancias)))
                .groupBy("DSProcesso").agg(F.sort_array(F.collect_set("s_std")).alias("subs")))
        cand = C.persist_disk(cand)
        n_candidatos = cand.count()

        # ---- 2) ... and a municipality in the target states (process → "MG,PA") ----------------------------
        pm = (tb["ProcessoMunicipio"].join(F.broadcast(cand.select("DSProcesso")), "DSProcesso", "left_semi")
              .join(F.broadcast(mun_fonte), "IDMunicipio").filter(F.col("uf_fonte") != ""))
        proc_ufs = pm.groupBy("DSProcesso").agg(F.sort_array(F.collect_set("uf_fonte")).alias("ufs"))
        foco_proc = cand.join(proc_ufs, "DSProcesso")
        if foco.ufs:
            foco_proc = foco_proc.filter(F.arrays_overlap("ufs", F.array(*[F.lit(u) for u in sorted(foco.ufs)])))
        foco_proc = C.persist_disk(foco_proc)
        n_foco = foco_proc.count()
        tl("processes with a focus substance: %d; in focus (substance + state): %d" % (n_candidatos, n_foco))
        if not n_foco:
            raise RuntimeError("No process in focus — snapshot not written (the current tables were kept)")
        keys = F.broadcast(foco_proc.select("DSProcesso"))

        def no_foco(name: str) -> DataFrame:
            return tb[name].join(keys, "DSProcesso", "left_semi")

        out: Dict[str, DataFrame] = {}

        # ---- 3) processos ----------------------------------------------------------------
        p = tb["Processo"]
        exige(p, "Processo.txt", ["DSProcesso", "NRProcesso", "NRAnoProcesso", "BTAtivo", "IDFaseProcesso", "DTProtocolo", "QTAreaHA"])
        p = p.join(F.broadcast(foco_proc), "DSProcesso")
        p = look(look(look(p, "IDFaseProcesso", dom["FaseProcesso"], "fase"), "IDTipoRequerimento", dom["TipoRequerimento"], "tipo_req"),
                 "IDUnidadeAdministrativaRegional", dom["UnidadeAdministrativaRegional"], "unidade")
        out["processo"] = p.select(
            F.col("DSProcesso").alias("processo"), chave_processo(F.col("DSProcesso")).alias("processo_chave"),
            int_or_null(col(p, "NRProcesso")).alias("nr_processo"), int_or_null(col(p, "NRAnoProcesso")).alias("ano_processo"),
            col(p, "NRNUP").alias("nup"),
            F.when(col(p, "BTAtivo") == "S", F.lit(1)).when(col(p, "BTAtivo") == "N", F.lit(0)).alias("ativo"),
            F.col("tipo_req").alias("tipo_requerimento"), "fase", F.col("unidade").alias("unidade_regional"),
            C.date_col(col(p, "DTProtocolo")).alias("dt_protocolo"), C.date_col(col(p, "DTPrioridade")).alias("dt_prioridade"),
            C.parse_num_col(col(p, "QTAreaHA")).alias("area_ha"), F.array_join("subs", ",").alias("substancias_foco"),
            F.array_join("ufs", ",").alias("ufs"))

        # ---- 4) substances -----------------------------------------------------------------
        s = no_foco("ProcessoSubstancia").join(sub_std, F.col("IDSubstancia") == sub_std["s_id"], "left")
        s = look(look(s, "IDTipoUsoSubstancia", dom["TipoUsoSubstancia"], "tipo_uso"),
                 "IDMotivoEncerramentoSubstancia", dom["MotivoEncerramentoSubstancia"], "motivo")
        std = F.coalesce(F.col("s_std"), F.lit("OUTROS"))
        out["substancia"] = s.select(
            F.col("DSProcesso").alias("processo"), C.h32(col(s, "IDSubstancia"), col(s, "IDTipoUsoSubstancia"), col(s, "DTInicioVigencia")).alias("chave"),
            chave_processo(F.col("DSProcesso")).alias("processo_chave"), int_or_null(col(s, "IDSubstancia")).alias("id_substancia"),
            F.col("s_norm").alias("substancia_origem"), std.alias("substancia"), C.prioridade_col(foco, std).alias("prioridade"),
            F.col("tipo_uso"), F.col("motivo").alias("motivo_encerramento"),
            C.date_col(col(s, "DTInicioVigencia")).alias("dt_inicio"), C.date_col(col(s, "DTFimVigencia")).alias("dt_fim"))

        # ---- 5) municipalities (IBGE code = the source's IDMunicipio) ------------------------------
        m = no_foco("ProcessoMunicipio").join(F.broadcast(mun_fonte), "IDMunicipio", "left")
        m = m.join(by_cod, m["IDMunicipio"] == by_cod["cod7"], "left")
        out["municipio"] = m.select(
            F.col("DSProcesso").alias("processo"), F.col("IDMunicipio").alias("cod_ibge"), chave_processo(F.col("DSProcesso")).alias("processo_chave"),
            F.col("uf_fonte").alias("uf"), F.coalesce("c_nome", "nm_fonte").alias("municipio"))

        # ---- 6) people: companies (PJ) with CNPJ and name; individuals (PF) aggregated per relation (LGPD)
        pp = no_foco("ProcessoPessoa")
        pessoa = tb["Pessoa"].join(F.broadcast(pp.select("IDPessoa").distinct()), "IDPessoa", "left_semi")
        doc = F.regexp_replace(C.s_("NRCPFCNPJ"), r"\D", "")
        pf_flag = (C.s_("TPPessoa") == "F") | ((C.s_("TPPessoa") != "J") & (F.length(doc) != 14)) | C.s_("NRCPFCNPJ").contains("*")
        pessoa = pessoa.select("IDPessoa", pf_flag.alias("p_pf"), C.pad14(doc).alias("p_doc"), C.s_("NMPessoa").alias("p_nome"))
        x = pp.join(pessoa, "IDPessoa", "left").withColumn("pf", F.coalesce(F.col("p_pf"), F.lit(True)))  # not in the registry: treated as PF
        x = look(x, "IDTipoRelacao", dom["TipoRelacao"], "rel_desc")
        x = x.withColumn("rel", F.coalesce("rel_desc", col(x, "IDTipoRelacao")))
        x = C.persist_disk(x.select("DSProcesso", "rel", "pf", "p_doc", "p_nome", col(x, "IDTipoRelacao").alias("id_rel"),
                                    col(x, "DTInicioVigencia").alias("ini"), col(x, "DTFimVigencia").alias("fim")))
        pj = x.filter(~F.col("pf")).select(
            F.col("DSProcesso").alias("processo"), C.h32("rel", "p_doc", "ini", "fim").alias("chave"),
            chave_processo(F.col("DSProcesso")).alias("processo_chave"), F.col("rel").alias("tipo_relacao"), F.lit("PJ").alias("tipo_pessoa"),
            F.col("p_doc").alias("documento"), F.substring("p_nome", 1, 300).alias("nome"), F.lit(1).alias("n_pessoas"),
            C.date_col(F.col("ini")).alias("dt_inicio"), C.date_col(F.col("fim")).alias("dt_fim"))
        pfa = x.filter(F.col("pf")).groupBy("DSProcesso", "rel", "ini", "fim").agg(F.count(F.lit(1)).alias("n"))
        pf = pfa.select(
            F.col("DSProcesso").alias("processo"), C.h32("rel", F.lit("PF"), "ini", "fim").alias("chave"),
            chave_processo(F.col("DSProcesso")).alias("processo_chave"), F.col("rel").alias("tipo_relacao"), F.lit("PF").alias("tipo_pessoa"),
            F.lit("PF").alias("documento"), F.lit(None).cast("string").alias("nome"), F.col("n").cast("int").alias("n_pessoas"),
            C.date_col(F.col("ini")).alias("dt_inicio"), C.date_col(F.col("fim")).alias("dt_fim"))
        out["pessoa"] = pj.unionByName(pf)
        pf_citada = F.broadcast(x.filter(F.col("pf") & F.col("id_rel").isin(*RELACOES_CITADAS_DOU)).select("DSProcesso").distinct()
                                .withColumn("_pf_dou", F.lit(True)))

        # ---- 7) titles -----------------------------------------------------------------------
        t = look(look(look(no_foco("ProcessoTitulo"), "IDDocumentoLegal", dom["DocumentoLegal"], "doc_legal"),
                      "IDTipoDocumentoLegal", dom["TipoDocumentoLegal"], "tipo_doc_legal"),
                 "IDSituacaoDocumentoLegal", dom["SituacaoDocumentoLegal"], "sit_doc_legal")
        out["titulo"] = t.select(
            F.col("DSProcesso").alias("processo"), C.h32(col(t, "NRTitulo"), col(t, "IDDocumentoLegal"), col(t, "DTPublicacao")).alias("chave"),
            chave_processo(F.col("DSProcesso")).alias("processo_chave"), col(t, "NRTitulo").alias("nr_titulo"),
            F.col("doc_legal").alias("documento_legal"), F.col("tipo_doc_legal").alias("tipo_documento_legal"),
            F.col("sit_doc_legal").alias("situacao"), C.date_col(col(t, "DTPublicacao")).alias("dt_publicacao"),
            C.date_col(col(t, "DTVencimento")).alias("dt_vencimento"))

        # ---- 8) events (≈1 GB uncompressed); exact duplicates collapse on the PK ------------------
        ev = look(no_foco("ProcessoEvento"), "IDEvento", dom["Evento"], "ev_desc").join(pf_citada, "DSProcesso", "left")
        pub = F.when(F.col("_pf_dou").isNotNull(), F.lit(None).cast("string")).otherwise(col(ev, "DSPublicacaoDOU"))
        out["evento"] = ev.select(
            F.col("DSProcesso").alias("processo"), C.h32(col(ev, "IDEvento"), col(ev, "DTEvento"), col(ev, "OBEvento")).alias("chave"),
            chave_processo(F.col("DSProcesso")).alias("processo_chave"), C.date_col(col(ev, "DTEvento")).alias("dt_evento"),
            int_or_null(col(ev, "IDEvento")).alias("id_evento"), F.substring("ev_desc", 1, 300).alias("evento"),
            F.substring(col(ev, "OBEvento"), 1, 2000).alias("ob_evento"), F.substring(pub, 1, 4000).alias("publicacao_dou"))

        carga = F.current_timestamp()
        final = {k: C.conform(df.withColumn("carga_em", carga).dropDuplicates(PK[k]), SCHEMA[k]) for k, df in out.items()}

        if profile:
            linhas = {T[k]: df.count() for k, df in final.items()}
            return dict(plano, profile=True, zip_mb=round(zip_bytes / 1048576), tabelas_zip=sorted(arquivos),
                        processos_com_substancia_foco=n_candidatos, processos_no_foco=n_foco, linhas=linhas,
                        vinculos_pf_agregados=x.filter("pf").count(), processos_com_pf_citada_no_dou=pf_citada.count(),
                        fases=[r.asDict() for r in final["processo"].groupBy("fase").count().orderBy(F.desc("count")).limit(30).collect()],
                        amostra_processo=[r.asDict() for r in final["processo"].limit(3).collect()], duracao_s=round(time.time() - t0))

        # ---- write: everything is staged before the first table is swapped ----------------------
        carregadas = {}
        for k, df in final.items():
            carregadas[T[k]] = C.stage(df, spark, target_db, T[k])
            tl("%s: %d" % (T[k], carregadas[T[k]]))
        for k in final:
            C.commit(spark, target_db, T[k], snapshot=True)
        ctl.ensure()
        ctl.write_file_info(FONTE, fi, zip_bytes, carregadas[T["evento"]], carregadas[T["evento"]])
        result = dict(plano, status="ok", processos_no_foco=n_foco, linhas_carregadas=carregadas,
                      vinculos_pf_agregados=x.filter("pf").count(), has_more=False, duracao_s=round(time.time() - t0),
                      concluido_em=C.utcnow_iso())
        ctl.write_state(FONTE, mode, None, "", "", "ok", result)
        print(result)
        return result
    except Exception as e:
        if not profile and ctl.exists():
            ctl.write_error(FONTE, mode, e)
        raise
    finally:
        C.cleanup(work)

# IBGE Municípios/Estados/Aliases (geolocalização).
# Source: github.com/kelvins/municipios-brasileiros (CSVs derived from the official IBGE bases, with centroids).
# Tables: dim_estadosIbge and dim_municipiosIbge (snapshot), dim_municipiosIbgeAliases (upsert by alias_norm + uf,
# so aliases added by hand survive a reload). The clima pipelines read the municipality centroids from here.

import io
import time
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession

from lakehouse import core as C

FONTE = "ibge_municipios"
BASE_URL = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv"
TB_EST, TB_MUN, TB_ALI = "dim_estadosIbge", "dim_municipiosIbge", "dim_municipiosIbgeAliases"

SCHEMA_EST = [("cod_uf", "string"), ("uf", "string"), ("estado", "string"), ("estado_norm", "string"), ("regiao", "string"),
              ("latitude", "decimal(10,6)"), ("longitude", "decimal(10,6)"), ("loaded_at", "timestamp")]
SCHEMA_MUN = [("cod_ibge", "string"), ("cod_ibge_5", "string"), ("cod_uf", "string"), ("uf", "string"), ("municipio", "string"),
              ("municipio_norm", "string"), ("municipio_uf", "string"), ("latitude", "decimal(10,6)"), ("longitude", "decimal(10,6)"),
              ("capital", "int"), ("siafi_id", "string"), ("ddd", "string"), ("fuso_horario", "string"), ("loaded_at", "timestamp")]
SCHEMA_ALI = [("alias_norm", "string"), ("uf", "string"), ("alias", "string"), ("cod_ibge", "string"), ("latitude", "decimal(10,6)"),
              ("longitude", "decimal(10,6)"), ("fonte", "string"), ("observacao", "string"), ("loaded_at", "timestamp")]

# Localities that are NOT IBGE municipalities (districts, beaches, regional offices) → the real municipality.
# The IBGE code is resolved by name from the downloaded base — never typed by hand — and the run fails if one does not resolve.
ALIAS_SEEDS = [
    {"alias": "Barreiro", "uf": "MG", "municipio": "Belo Horizonte", "fonte": "ABSOLUTE", "observacao": "Regional de Belo Horizonte"},
    {"alias": "Farol de Sao Thome", "uf": "RJ", "municipio": "Campos dos Goytacazes", "fonte": "ABSOLUTE",
     "observacao": "Distrito de Campos dos Goytacazes"},
    {"alias": "Porto de Galinhas", "uf": "PE", "municipio": "Ipojuca", "fonte": "ABSOLUTE", "observacao": "Praia e distrito de Ipojuca"},
]


def upper_strip(s) -> str:
    """no accents, upper case, trimmed (punctuation kept)"""
    return C.strip_accents(s).upper().strip()


def fetch(name: str, base_url: str) -> pd.DataFrame:
    text = C.http_bytes("%s/%s.csv" % (base_url, name)).decode("utf-8-sig")
    return pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)


def build(municipios: pd.DataFrame, estados: pd.DataFrame):
    if len(municipios) < 5000 or len(estados) != 27:
        raise ValueError("Fonte suspeita: %d municípios / %d estados" % (len(municipios), len(estados)))
    uf_por_cod = dict(zip(estados["codigo_uf"].astype(str), estados["uf"]))
    est = pd.DataFrame({"cod_uf": estados["codigo_uf"].str.zfill(2), "uf": estados["uf"], "estado": estados["nome"],
                        "estado_norm": estados["nome"].map(upper_strip), "regiao": estados["regiao"],
                        "latitude": estados["latitude"], "longitude": estados["longitude"]})
    cod = municipios["codigo_ibge"].str.strip()
    uf = municipios["codigo_uf"].astype(str).map(uf_por_cod).fillna("")
    nome = municipios["nome"].str.strip()
    mun = pd.DataFrame({"cod_ibge": cod, "cod_ibge_5": cod.str[2:], "cod_uf": municipios["codigo_uf"].str.zfill(2), "uf": uf,
                        "municipio": nome, "municipio_norm": nome.map(upper_strip), "municipio_uf": nome + " - " + uf,
                        "latitude": municipios["latitude"], "longitude": municipios["longitude"],
                        "capital": municipios["capital"].replace("", "0"), "siafi_id": municipios["siafi_id"],
                        "ddd": municipios["ddd"], "fuso_horario": municipios["fuso_horario"]})
    idx = {(n, u): c for n, u, c in zip(mun["municipio_norm"], mun["uf"], mun["cod_ibge"])}
    ali, faltam = [], []
    for a in ALIAS_SEEDS:  # validated BEFORE anything is written
        c = idx.get((upper_strip(a["municipio"]), a["uf"]))
        if not c:
            faltam.append("%s -> %s/%s" % (a["alias"], a["municipio"], a["uf"]))
            continue
        ali.append({"alias_norm": upper_strip(a["alias"]), "uf": a["uf"], "alias": a["alias"], "cod_ibge": c,
                    "latitude": a.get("latitude"), "longitude": a.get("longitude"), "fonte": a.get("fonte"), "observacao": a.get("observacao")})
    if faltam:
        raise ValueError("Aliases sem município correspondente no IBGE: " + "; ".join(faltam))
    return est, mun, pd.DataFrame(ali, columns=[c for c, _ in SCHEMA_ALI if c != "loaded_at"])


def to_spark(spark: SparkSession, pdf: pd.DataFrame, schema):
    cols = [c for c, _ in schema if c != "loaded_at"]
    df = spark.createDataFrame(pdf[cols].astype(object).where(pdf[cols].notna(), None), ", ".join("%s string" % c for c in cols))
    return C.conform(df, schema)


def main(spark: Optional[SparkSession] = None, base_url: str = BASE_URL, dry_run: bool = False, target_db: str = "estudo"):
    t0 = time.time()
    municipios, estados = fetch("municipios", base_url), fetch("estados", base_url)
    est, mun, ali = build(municipios, estados)
    plano = {"fonte": FONTE, "url": base_url, "destino": ["%s.%s" % (target_db, t) for t in (TB_EST, TB_MUN, TB_ALI)],
             "estados": len(est), "municipios": len(mun), "aliases_seed": len(ali)}
    print("[IBGE] %s" % plano)
    if dry_run:
        return dict(plano, dry_run=True, sem_coordenada=int(((mun["latitude"] == "") | (mun["longitude"] == "")).sum()))
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")
    ctl = C.Ctl(target_db)
    try:
        n_est = C.load(spark, to_spark(spark, est, SCHEMA_EST), target_db, TB_EST, snapshot=True)
        n_mun = C.load(spark, to_spark(spark, mun, SCHEMA_MUN), target_db, TB_MUN, snapshot=True)
        C.load(spark, to_spark(spark, ali, SCHEMA_ALI), target_db, TB_ALI, pk=["alias_norm", "uf"])
        t_mun = spark.read.parquet(C.uri(C.table_path(target_db, TB_MUN)))
        t_ali = spark.read.parquet(C.uri(C.table_path(target_db, TB_ALI)))
        sem_coord = t_mun.filter("latitude IS NULL OR longitude IS NULL").count()
        orfaos = t_ali.join(t_mun.select("cod_ibge"), "cod_ibge", "left_anti").count()
        result = dict(plano, status="ok", estados_carregados=n_est, municipios_carregados=n_mun, aliases_na_tabela=t_ali.count(),
                      sem_coordenada=sem_coord, aliases_orfaos=orfaos, duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
        if sem_coord or orfaos:
            raise ValueError("Carga inconsistente: %d municípios sem coordenada, %d aliases apontando para código IBGE inexistente"
                             % (sem_coord, orfaos))
        ctl.write_state(FONTE, "snapshot", "ok", result)
        print(result)
        return result
    except Exception as e:
        ctl.write_error(FONTE, "snapshot", e)
        raise

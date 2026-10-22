# ANP — oil and natural gas production by well → <target_db>.anp_producao_poco.

import os
import re
import time
import urllib.parse
import zipfile
from functools import reduce
from typing import Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import common as C

FONTE = "anp_poco"
TABLE = "anp_producao_poco"
VIEW = "anp_producao_instalacao"
FIRST_YM = "2015-01"
PAGINA_PADRAO = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/producao-de-petroleo-e-gas-natural-por-poco"
NUM_COLS = ["oleo", "condensado", "petroleo", "gas_associado", "gas_nao_associado", "gas_total", "gas_royalties", "agua", "tempo_producao"]

SCHEMA = ([("ano_mes", "date"), ("poco", "string"), ("poco_operador", "string"), ("uf", "string"), ("estado", "string"),
           ("bacia", "string"), ("campo", "string"), ("operador", "string"), ("contrato", "string"), ("instalacao", "string"),
           ("tipo_instalacao", "string"), ("localizacao", "string")]
          + [(c, "decimal(10,2)" if c == "tempo_producao" else "decimal(20,4)") for c in NUM_COLS]
          + [("corrente", "string"), ("arquivo_origem", "string"), ("loaded_at", "timestamp")])

SPEC = [
    ("periodo", r"^PERIODO$|^PERIODODEPRODUCAO|^MESANO|^ANOMES|^DATAPRODUCAO|^DATA$|^MESREFERENCIA"),
    ("ano", r"^ANO$"),
    ("mes", r"^MES$"),
    ("estado", r"^ESTADO$|^UF$|^SIGLAUF"),
    ("bacia", r"^BACIA"),
    ("poco_operador", r"POCOOPERADOR|POCODOOPERADOR"),
    ("poco", r"^NOMEPOCOANP|^POCOANP|^NOMEDOPOCOANP|^NOMEPOCO$|^NOMEDOPOCO$|^POCO$"),
    ("campo", r"^CAMPO|^NOMEDOCAMPO|^NOMECAMPO"),
    ("operador", r"^OPERADOR"),
    ("contrato", r"CONTRATO"),
    ("tipo_instalacao", r"^TIPOINSTALACAO|^TIPODEINSTALACAO|^TIPODAINSTALACAO"),
    ("instalacao", r"^INSTALACAO|^NOMEINSTALACAO|^NOMEDAINSTALACAO"),
    ("localizacao", r"^LOCALIZACAO|^AMBIENTE|^TERRAMAR|^LOCAL$"),
    ("gas_royalties", r"ROYALT"),
    ("oleo", r"^OLEO"),
    ("condensado", r"^CONDENSADO"),
    ("petroleo", r"^PETROLEO"),
    ("gas_nao_associado", r"^GAS.*NAOASSOC"),
    ("gas_associado", r"^GAS.*ASSOC"),
    ("gas_total", r"^GAS.*TOTAL|^GASNATURAL(MMDIA|MM3DIA)?$"),
    ("agua", r"^AGUA"),
    ("tempo_producao", r"^TEMPO"),
    ("corrente", r"^CORRENTE"),
]

UF_NOME = {"ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAZONAS": "AM", "BAHIA": "BA", "CEARA": "CE", "DISTRITO FEDERAL": "DF",
           "ESPIRITO SANTO": "ES", "GOIAS": "GO", "MARANHAO": "MA", "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS",
           "MINAS GERAIS": "MG", "PARA": "PA", "PARAIBA": "PB", "PARANA": "PR", "PERNAMBUCO": "PE", "PIAUI": "PI",
           "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS", "RONDONIA": "RO", "RORAIMA": "RR",
           "SANTA CATARINA": "SC", "SAO PAULO": "SP", "SERGIPE": "SE", "TOCANTINS": "TO"}
MESES_EXTENSO = ["JANEIRO", "FEVEREIRO", "MARCO", "ABRIL", "MAIO", "JUNHO", "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"]


def link_periodo(url: str) -> Optional[dict]:
    """period covered by a link, from the file/folder names (one month, or a whole year)"""
    try:
        path = urllib.parse.unquote(urllib.parse.urlparse(url).path)
    except Exception:
        path = url
    file = path.split("/")[-1] or path
    for s in (file, path):
        m = re.search(r"(?<!\d)((?:19|20)\d{2})[-_.]?(0[1-9]|1[0-2])(?!\d)", s)
        if m:
            ym = "%s-%s" % (m.group(1), m.group(2))
            return {"ini": ym, "fim": ym, "nivel": "mes"}
        m = re.search(r"(?<!\d)(0[1-9]|1[0-2])[-_.]((?:19|20)\d{2})(?!\d)", s)
        if m:
            ym = "%s-%s" % (m.group(2), m.group(1))
            return {"ini": ym, "fim": ym, "nivel": "mes"}
        toks = C.norm(s).split(" ")
        y = next((t for t in toks if re.match(r"^(19|20)\d{2}$", t)), None)
        mo = next((MESES_EXTENSO.index(t) + 1 for t in toks if t in MESES_EXTENSO), None)
        if y and mo:
            ym = "%s-%02d" % (y, mo)
            return {"ini": ym, "fim": ym, "nivel": "mes"}
        if y:
            return {"ini": "%s-01" % y, "fim": "%s-12" % y, "nivel": "ano"}
    return None


def descobre_links(pagina: str, filtro: "re.Pattern") -> dict:
    html = C.http_text(pagina)
    hrefs = []
    for m in re.finditer(r"""href\s*=\s*["']([^"'#]+)["']""", html, re.I):
        try:
            u = urllib.parse.urljoin(pagina, m.group(1).replace("&amp;", "&"))
        except Exception:
            continue
        if re.search(r"\.(csv|zip)(/@@download.*|/view)?(\?.*)?$", u, re.I) and filtro.search(urllib.parse.unquote(u)):
            u = re.sub(r"/view$", "", u, flags=re.I)
            if u not in hrefs:
                hrefs.append(u)
    links, sem = [], []
    for u in hrefs:
        p = link_periodo(u)
        if p:
            links.append(dict(url=u, **p))
        else:
            sem.append(u)
    links.sort(key=lambda l: l["ini"])
    return {"links": links, "sem_periodo": sem, "total": len(hrefs)}


def _arquivos(path: str, url: str) -> List[str]:
    """CSV/TXT files of a downloaded link (a ZIP is extracted next to it)"""
    if not (re.search(r"\.zip(/|\?|$)", url, re.I) or zipfile.is_zipfile(path)):
        return [path]
    out_dir = path + ".unzipped"
    os.makedirs(out_dir, exist_ok=True)
    out = []
    with zipfile.ZipFile(path) as z:
        for info in z.infolist():
            if re.search(r"\.(csv|txt)$", info.filename, re.I):
                dest = os.path.join(out_dir, os.path.basename(info.filename))
                with z.open(info) as src, open(dest, "wb") as dst:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        dst.write(chunk)
                out.append(dest)
    return out


def _acha_cabecalho(path: str):
    """(idx, header, delim, linha_fisica): header = first line (≤30 non-empty) mapping both 'poco' and 'campo'"""
    nao_vazias = 0
    for fis, line in enumerate(C.head_lines(path, 400)):
        if not line.strip():
            continue
        d = C.detect_delim(line)
        h = C.split_csv(line, d)
        idx, livres = C.map_header(h, SPEC)
        if "poco" in idx and "campo" in idx:
            return idx, h, d, fis, livres
        nao_vazias += 1
        if nao_vazias > 30:
            raise ValueError("Header (well + field) not found in the first 30 lines of %s. Last line: %s" % (path, line[:300]))
    raise ValueError("File without a recognizable header: %s" % path)


def _a_partir_da_linha(path: str, fis: int) -> str:
    """the CSV reader needs the header on line 1: copy the file without the preamble when there is one"""
    if fis == 0:
        return path
    dest = path + ".clean"
    with open(path, "rb") as src, open(dest, "wb") as dst:
        for i, raw in enumerate(src):
            if i >= fis:
                dst.write(raw)
    return dest


def _faltam(idx) -> List[str]:
    return [c for c in ("poco", "campo") if c not in idx]


def main(spark: Optional[SparkSession] = None, mode: str = "incremental", reference_date: str = "", start_date: str = "",
         end_date: str = "", lookback_meses: int = 3, max_periodos: int = 12, pagina: str = PAGINA_PADRAO,
         url_filtro: str = "po[cç]o|producao|produ[cç][aã]o", urls: Optional[List[str]] = None, force: bool = False,
         reset_cursor: bool = False, excluir_presal: bool = True, profile: bool = False, dry_run: bool = False,
         target_db: str = "bronze", cfg_db: str = ""):
    t0 = time.time()
    if mode not in ("incremental", "range", "backfill"):
        raise ValueError("invalid mode: %s" % mode)
    if lookback_meses < 1 or max_periodos < 1:
        raise ValueError("lookback_meses e max_periodos devem ser >= 1")
    ref = C.to_ym("reference_date", reference_date) or C.today_local()[:7]
    s_ym, e_ym = C.to_ym("start_date", start_date), C.to_ym("end_date", end_date)
    pagina = pagina or PAGINA_PADRAO

    cfg = ""
    for db in [d for d in (cfg_db, target_db, "silver") if d]:
        if C.table_exists(db, "dim_alvoExtracao"):
            cfg = db
            break
    foco = C.load_foco(cfg, FONTE, True)
    ctl = C.Ctl(target_db)

    # ---- window -----------------------------------------------------------
    wm = C.watermark_ym(target_db, TABLE)
    has_more = False
    if mode == "range":
        if not s_ym or not e_ym:
            raise ValueError("mode=range requires start_date and end_date")
        ini = win_ini = s_ym
        fim = win_fim = e_ym
    elif mode == "incremental":
        win_fim = e_ym or ref
        lb = C.add_months(win_fim, -(lookback_meses - 1))  # empty table: only the last lookback_meses (history = backfill)
        win_ini = s_ym or (min(lb, C.add_months(wm, 1)) if wm else lb)
        ini, fim = win_ini, win_fim
    else:
        win_ini, win_fim = s_ym or FIRST_YM, e_ym or ref
        st = ctl.read_state(FONTE, "backfill") if ctl.exists() else None
        ini = win_ini
        if st and not reset_cursor and st.get("janela_inicio") == win_ini and st.get("janela_fim") == win_fim and st.get("cursor_periodo"):
            ini = C.add_months(str(st["cursor_periodo"]), 1)
        fim = min(C.add_months(ini, max_periodos - 1), win_fim)
        has_more = fim < win_fim

    # ---- links ------------------------------------------------------------
    if urls:
        disc = {"links": [dict(url=u, **(link_periodo(u) or {"ini": "0000-01", "fim": "9999-12", "nivel": "ano"})) for u in urls],
                "sem_periodo": [], "total": len(urls)}
    else:
        disc = descobre_links(pagina, re.compile(url_filtro, re.I))
    alvo = [l for l in disc["links"] if l["fim"] >= ini and l["ini"] <= fim] if ini <= fim else []
    plano = {"fonte": FONTE, "mode": mode, "janela": "%s..%s" % (win_ini, win_fim), "lote": "%s..%s" % (ini, fim), "watermark": wm,
             "destino": "%s.%s" % (target_db, TABLE), "bacias": foco.bacias, "links_encontrados": disc["total"],
             "links_com_periodo": len(disc["links"]),
             "cobertura_links": ("%s..%s" % (disc["links"][0]["ini"], disc["links"][-1]["fim"])) if disc["links"] else None,
             "arquivos_do_lote": [l["url"] for l in alvo], "has_more": has_more}
    if not alvo and disc["links"] and ini <= fim and fim > disc["links"][-1]["fim"]:
        # files are discovered by scraping the page, and nothing newer than the last link exists there
        plano["aviso"] = ("the window ends at %s but the page links only cover up to %s — no file will be read and the load ends 'ok' "
                          "with 0 rows (ANP publishes 2024 onward only in its Public Data Query, without file links)"
                          % (fim, disc["links"][-1]["fim"]))
    print("[ANP] %s" % plano)
    if dry_run:
        return dict(plano, dry_run=True, links_sem_periodo=disc["sem_periodo"][:30])
    if not disc["links"]:
        raise RuntimeError("No CSV/ZIP link with a period found on %s (filter /%s/). Did the page change? Links without a period: %s"
                           % (pagina, url_filtro, " , ".join(disc["sem_periodo"][:10])))
    if spark is None:
        raise ValueError("spark is required unless dry_run=True")

    baixados: List[str] = []
    try:
        # ---- leitura -----------------------------------------------------------
        alvo_proc = alvo[-1:] if profile else alvo
        arquivos, frames, infos = [], [], []
        descartes_arquivo: Dict[str, int] = {}
        for l in alvo_proc:
            fi = C.head_info(l["url"])
            if not force and not profile and ctl.exists() and C.unchanged(ctl.read_file_info(FONTE, l["url"]), fi):
                arquivos.append({"url": l["url"], "status": "sem_mudanca"})
                time.sleep(C.PAUSA_S)
                continue
            path, nbytes = C.download(l["url"], FONTE)
            baixados += [path, path + ".unzipped"]
            for arq in _arquivos(path, l["url"]):
                if excluir_presal and re.search(r"pre[-_ ]?sal", os.path.basename(arq), re.I):
                    C.bump(descartes_arquivo, "presal_ignorado")  # subset of Mar: counting it would double every pre-salt well
                    continue
                idx, header, delim, fis, livres = _acha_cabecalho(arq)
                src = _a_partir_da_linha(arq, fis)
                if src != arq:
                    baixados.append(src)
                raw = C.read_csv(spark, src, delim, C.detect_encoding(src))
                g = C.col_getter(idx)
                if "periodo" in idx:
                    ym = C.parse_periodo_col(g("periodo"))
                elif "ano" in idx and "mes" in idx:
                    ym = C.parse_periodo_col(F.concat(g("ano"), F.lit("-"), F.lpad(g("mes"), 2, "0")))
                elif l["nivel"] == "mes":
                    ym = F.lit(l["ini"])
                else:
                    ym = F.lit(None).cast("string")
                bacia = C.norm_col(g("bacia"))
                poco = F.upper(F.regexp_replace(g("poco"), r"\s+", " ")).substr(1, 80)
                if foco.bacias:
                    fora_foco = (bacia != "") & ~reduce(lambda a, b: a | b, [bacia.contains(b) for b in foco.bacias])
                else:
                    fora_foco = F.lit(False)
                descarte = (F.when(ym.isNull(), "sem_periodo").when((ym < ini) | (ym > fim), "fora_do_lote")
                            .when(fora_foco, "fora_do_foco").when(poco == "", "sem_poco"))
                est = C.norm_col(g("estado"))
                uf_map = F.create_map(*[x for k, v in UF_NOME.items() for x in (F.lit(k), F.lit(v))])
                cols = [F.lit(l["url"]).alias("_arq"), ym.alias("ym"), poco.alias("poco"), descarte.alias("descarte"),
                        g("poco_operador").alias("poco_operador"), F.when(F.length(est) == 2, est).otherwise(uf_map[est]).alias("uf"),
                        g("estado").alias("estado"), g("bacia").alias("bacia"), g("campo").alias("campo"), g("operador").alias("operador"),
                        g("contrato").alias("contrato"), g("instalacao").alias("instalacao"), g("tipo_instalacao").alias("tipo_instalacao"),
                        g("localizacao").alias("localizacao"), g("corrente").alias("corrente"), F.lit(l["url"][:500]).alias("arquivo_origem")]
                cols += [(C.parse_num_col(g(n)) if n in idx else F.lit(None).cast("double")).alias(n) for n in NUM_COLS]
                frames.append(raw.select(*cols))
                if profile:
                    arquivos.append({"url": l["url"], "arquivo": os.path.basename(arq), "delimitador": delim, "cabecalho": header,
                                     "mapeamento": idx, "nao_usadas": livres})
            fi["_bytes"] = nbytes
            infos.append((l, fi))
            time.sleep(C.PAUSA_S)

        tagged = None
        descartes: Dict[str, int] = {}
        if frames:
            tagged = frames[0]
            for f in frames[1:]:
                tagged = tagged.unionByName(f)
            tagged = C.persist_disk(tagged)
            cont = {(r["_arq"], r["descarte"]): r["n"] for r in tagged.groupBy("_arq", "descarte").agg(F.count(F.lit(1)).alias("n")).collect()}
            for (arq, desc), n in cont.items():
                if desc:
                    C.bump(descartes, desc, n)
            for l, fi in infos:
                lidas = sum(n for (a, _), n in cont.items() if a == l["url"])
                no_foco = cont.get((l["url"], None), 0)
                fi["_lidas"] = lidas
                if not profile:
                    arquivos.append({"url": l["url"], "status": "lido", "mb": round(fi["_bytes"] / 1048576, 1), "linhas_lidas": lidas,
                                     "linhas_no_foco": no_foco})

        if tagged is not None:
            kept = tagged.filter(F.col("descarte").isNull())
            agg = (kept.groupBy("ym", "poco").agg(
                *[F.first(c).alias(c) for c in ("poco_operador", "uf", "estado", "bacia", "campo", "operador", "contrato", "instalacao",
                                                 "tipo_instalacao", "localizacao", "corrente", "arquivo_origem")],
                *[(F.max(n) if n == "tempo_producao" else F.sum(n)).alias(n) for n in NUM_COLS]))
            agg = agg.withColumn("ano_mes", F.to_date(F.concat("ym", F.lit("-01")))).drop("ym")
            for n in NUM_COLS:
                agg = agg.withColumn(n, F.round(n, 4))
            out = C.conform(agg, SCHEMA)
        else:
            out = None

        if profile:
            return dict(plano, profile=True, links=disc["links"][-24:], links_sem_periodo=disc["sem_periodo"][:30], arquivos=arquivos,
                        descartes=descartes, pocos_no_foco=out.count() if out is not None else 0,
                        amostra=[r.asDict() for r in out.limit(5).collect()] if out is not None else [], duracao_s=round(time.time() - t0))

        # ---- write -------------------------------------------------------------
        ctl.ensure()
        n = 0
        if out is not None:
            n = C.load(spark, out, target_db, TABLE, pk=["ano_mes", "poco"])
            _materializa_instalacao(spark, target_db)
        for l, fi in infos:
            ctl.write_file_info(FONTE, fi, fi.get("_bytes", 0), fi.get("_lidas", 0), n)
        descartes.update(descartes_arquivo)
        result = dict(plano, status="ok", arquivos=arquivos, descartes=descartes, linhas_carregadas=n, cursor=fim, has_more=has_more,
                      duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
        ctl.write_state(FONTE, mode, fim, win_ini, win_fim, "ok", result)
        print(result)
        return result
    except Exception as e:
        if not profile and ctl.exists():
            ctl.write_error(FONTE, mode, e)
        raise
    finally:
        C.cleanup(*baixados)


def _materializa_instalacao(spark: SparkSession, db: str) -> None:
    """anp_producao_instalacao: monthly sum of the wells per installation (materialized on every load)"""
    p = spark.read.parquet(C.uri(C.table_path(db, TABLE))).filter(F.col("instalacao").isNotNull())
    v = (p.groupBy("ano_mes", "instalacao").agg(
        F.max("tipo_instalacao").alias("tipo_instalacao"), F.max("uf").alias("uf"), F.max("bacia").alias("bacia"),
        F.max("operador").alias("operador"), F.countDistinct("campo").alias("n_campos"), F.countDistinct("poco").alias("n_pocos"),
        *[F.sum(c).alias(c) for c in ("oleo", "condensado", "petroleo", "gas_associado", "gas_nao_associado", "gas_total", "agua")]))
    C.load(spark, v, db, VIEW, snapshot=True)

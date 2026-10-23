# MTE — Cadastro de Empregadores ("Lista Suja" do trabalho análogo à escravidão).
# Official spreadsheet (stable URL, no auth; HEAD answers 403 but GET works), updated ~twice a year (April, October);
# an employer stays listed for 2 years. It is a report export: title block, header on row ~8 with merged cells,
# footnote markers "(*n)" in employer names, a page-number artifact. The parser finds the header by the "ID" cell,
# validates it, reads only the header column positions and keeps rows with a numeric ID.
# Table <target_db>.mte_cadastro_empregadores, upsert by chave_registro = md5(doc | year | establishment | workers |
# decision) — the spreadsheet ID is renumbered every edition. data_referencia = last edition in which the row was seen
# (rows that left the list are never deleted: filter on MAX(data_referencia) for the current list).
# LGPD: the source publishes FULL CPFs of individual employers. By default only the
# Receita-style masked CPF (***######**, the format of cnpj_socios) is stored: cnpj_cpf and cpf_cnpj_norm carry it
# for individuals. The key is still computed from the full document, so it does not change with the masking.
# manter_cpf_aberto=True stores the open CPF.

import hashlib
import re
import time
import unicodedata
from datetime import date, datetime, time as dtime
from io import BytesIO
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession

from lakehouse import core as C

FONTE = "mte_lista_suja"
TABLE = "mte_cadastro_empregadores"
SOURCE_URL = ("https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/areas-de-atuacao/"
              "cadastro_de_empregadores.xlsx")
MIN_ROWS = 100  # the list has hundreds of rows; fewer = truncated spreadsheet

SOURCE_COLUMNS = [
    ("ID", "id_publicacao"), ("Ano da ação fiscal", "ano_da_acao_fiscal"), ("UF", "uf"), ("Empregador", "empregador"),
    ("CNPJ/CPF", "cnpj_cpf"), ("Estabelecimento", "estabelecimento"), ("Trabalhadores envolvidos", "trabalhadores_envolvidos"),
    ("CNAE", "cnae"), ("Decisão administrativa de procedência", "decisao_administrativa_de_procedencia"),
    ("Inclusão no Cadastro de Empregadores", "inclusao_no_cadastro_de_empregadores"),
]
TABLE_COLUMNS = (["chave_registro", "cpf_cnpj_norm", "tipo_pessoa", "cnpj_basico", "cpf_mascarado_rf"]
                 + [c for _, c in SOURCE_COLUMNS] + ["nota_rodape", "edicao_periodica", "data_referencia"])
MESES = {"janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4, "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
         "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12}


def norm_text(text) -> str:
    t = "".join(c for c in unicodedata.normalize("NFD", str(text)) if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", re.sub(r"[^A-Za-z0-9]+", " ", t)).strip().upper()


def norm_cpf_cnpj(raw) -> Optional[str]:
    s = re.sub(r"\D", "", "" if raw is None else str(raw))
    return s if len(s) in (11, 14) else None


def masked_cpf_rf(n: Optional[str]) -> Optional[str]:
    return "***%s**" % n[3:9] if n and len(n) == 11 else None


def cell_text(v) -> Optional[str]:
    """cell → text as the source shows it: dates DD/MM/AAAA, integers without '.0'"""
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.strftime("%d/%m/%Y")
    if isinstance(v, dtime):  # badly formatted date cell in the source (time only) → no value
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    return s or None


def parse_edition_dates(title_rows: List[str]) -> Tuple[str, str]:
    """'Atualização periódica de 6 de abril de 2026. Cadastro atualizado em 04/09/2026.' → ('2026-04-06', '2026-09-04')"""
    periodic = updated = None
    for t in title_rows:
        m = re.search(r"(\d{1,2})\s+de\s+([A-Za-zçÇ]+)\s+de\s+(\d{4})", t)
        if m and m.group(2).lower() in MESES and periodic is None:
            periodic = date(int(m.group(3)), MESES[m.group(2).lower()], int(m.group(1))).isoformat()
        m2 = re.search(r"atualizado\s+em\s+(\d{2})/(\d{2})/(\d{4})", t, re.I)
        if m2 and updated is None:
            updated = "%s-%s-%s" % (m2.group(3), m2.group(2), m2.group(1))
    if periodic is None:
        raise ValueError("edition date not found in title rows: %s" % title_rows[:6])
    return periodic, updated or periodic


def parse_workbook(content: bytes, manter_cpf_aberto: bool = False) -> Tuple[str, List[dict]]:
    import openpyxl
    wb = openpyxl.load_workbook(BytesIO(content), read_only=True, data_only=True)
    rows = [tuple(r) for r in wb.worksheets[0].iter_rows(values_only=True)]
    hdr_idx = next((i for i, r in enumerate(rows) if r and r[0] is not None and norm_text(r[0]) == "ID"), None)
    if hdr_idx is None:
        raise ValueError("header row (cell 'ID') not found — layout changed?")
    hdr = rows[hdr_idx]
    positions = [j for j, h in enumerate(hdr) if h is not None]
    got, expected = [norm_text(hdr[j]) for j in positions], [norm_text(h) for h, _ in SOURCE_COLUMNS]
    if got != expected:
        raise ValueError("source header changed — refusing to load.\n expected %s\n got      %s" % (expected, got))
    titles = [str(r[0]) for r in rows[:hdr_idx] if r and r[0] is not None]
    periodic, edition = parse_edition_dates(titles)
    print("[MTE] header OK at row %d, columns at %s | edicao_periodica=%s data_referencia=%s" % (hdr_idx + 1, positions, periodic, edition))
    records = []
    for r in rows[hdr_idx + 1:]:
        if not r or r[0] is None:
            continue
        c0 = r[0]
        if not (isinstance(c0, (int, float)) or (isinstance(c0, str) and c0.strip().isdigit())):
            continue
        r = r + (None,) * (max(positions) + 1 - len(r))
        rec = {col: cell_text(r[j]) for (_, col), j in zip(SOURCE_COLUMNS, positions)}
        mk = re.search(r"\(\*\d+\)", rec["empregador"] or "")
        n = norm_cpf_cnpj(rec["cnpj_cpf"])
        key_src = "|".join([n or norm_text(re.sub(r"\(\*\d+\)", "", rec["empregador"] or "")), rec["ano_da_acao_fiscal"] or "",
                            norm_text(rec["estabelecimento"] or ""), rec["trabalhadores_envolvidos"] or "",
                            rec["decisao_administrativa_de_procedencia"] or ""])
        rec["chave_registro"] = hashlib.md5(key_src.encode("utf-8")).hexdigest()  # name without the (*n) marker
        rec["tipo_pessoa"] = "J" if n and len(n) == 14 else ("F" if n else None)
        rec["cnpj_basico"] = n[:8] if n and len(n) == 14 else None
        rec["cpf_mascarado_rf"] = masked_cpf_rf(n)
        if rec["tipo_pessoa"] == "F" and not manter_cpf_aberto:
            rec["cpf_cnpj_norm"] = rec["cnpj_cpf"] = rec["cpf_mascarado_rf"]
        else:
            rec["cpf_cnpj_norm"] = n
        rec["nota_rodape"] = mk.group(0) if mk else None
        rec["edicao_periodica"], rec["data_referencia"] = periodic, edition
        records.append(rec)
    print("[MTE] data rows: %d (PJ %d, PF %d, sem doc %d, com nota de rodapé %d)"
          % (len(records), sum(x["tipo_pessoa"] == "J" for x in records), sum(x["tipo_pessoa"] == "F" for x in records),
             sum(x["tipo_pessoa"] is None for x in records), sum(bool(x["nota_rodape"]) for x in records)))
    if len(records) < MIN_ROWS:
        raise ValueError("only %d rows parsed (< min_rows=%d) — refusing to load" % (len(records), MIN_ROWS))
    return edition, records


def main(spark: Optional[SparkSession] = None, full_refresh: bool = False, manter_cpf_aberto: bool = False, dry_run: bool = False,
         target_db: str = "estudo", url: str = SOURCE_URL):
    t0 = time.time()
    content = C.http_bytes(url, headers={"User-Agent": "Mozilla/5.0 (compatible; pocket-lakehouse/1.0)"})
    if content[:2] != b"PK":
        raise RuntimeError("downloaded body is not an XLSX (zip) file")
    edition, records = parse_workbook(content, manter_cpf_aberto)
    keys = [x["chave_registro"] for x in records]
    dups = len(keys) - len(set(keys))
    if dups:
        print("[MTE] WARNING: %d duplicate keys within the edition; last one wins" % dups)
    plano = {"fonte": FONTE, "edition": edition, "rows_parsed": len(records), "duplicadas": dups, "destino": "%s.%s" % (target_db, TABLE),
             "cpf_aberto": manter_cpf_aberto}
    if dry_run:
        return dict(plano, success=True, dry_run=True, duracao_s=round(time.time() - t0))
    if spark is None:
        raise ValueError("spark é obrigatório fora do dry_run")
    last = {}
    for rec in records:
        last[rec["chave_registro"]] = rec
    rows = [tuple(r.get(c) for c in TABLE_COLUMNS) for r in last.values()]
    df = C.conform(spark.createDataFrame(rows, ", ".join("%s string" % c for c in TABLE_COLUMNS)),
                   [(c, "string") for c in TABLE_COLUMNS] + [("loaded_at", "timestamp")])
    before = len(C.read_table_pd(target_db, TABLE, ["chave_registro"])) if C.table_exists(target_db, TABLE) else 0
    n = C.load(spark, df, target_db, TABLE, pk=None if full_refresh else ["chave_registro"], snapshot=full_refresh)
    t = C.read_table_pd(target_db, TABLE, ["chave_registro", "data_referencia"])
    result = dict(plano, success=True, rows_loaded=n, table_rows_before=before, table_rows_after=len(t),
                  rows_in_edition=int((t["data_referencia"] == edition).sum()), duracao_s=round(time.time() - t0), concluido_em=C.utcnow_iso())
    C.Ctl(target_db).write_state(FONTE, "full_refresh" if full_refresh else "upsert", "ok", result)
    print(result)
    return result

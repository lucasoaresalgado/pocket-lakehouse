# Offline tests: every pipeline against small fixtures (file:// zips, a generated xlsx and 7z, a mocked API), no network.

# Run inside the jupyter container:  cd /notebooks/empresas && python3 -m unittest tests.test_pipelines
# (the MTE and CAGED cases need openpyxl / py7zr from Dockerfile.jupyter and are skipped without them)

import io
import os
import shutil
import sys
import time
import unittest
import zipfile

os.environ["DATA_ROOT"] = "/data/_test_empresas"
os.environ["ALLOW_LOCAL_STORAGE"] = "1"  # local-disk mode is blocked outside the tests
os.environ["STORAGE"] = "file"
os.environ["PAUSA_S"] = "0"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession

from pipelines import caged, cgu, cnpj, cvm, flow, mte, pgfn, pncp
from lakehouse import core as C

ROOT = os.environ["DATA_ROOT"]
FIX = os.path.join(ROOT, "fixtures")
DB = "t_emp"
time.sleep = lambda s: None  # retries/back-offs of the HTTP layer must not slow the tests down
shutil.rmtree(ROOT, ignore_errors=True)
cgu.PROBE_PAUSE_S = 0

try:
    import openpyxl  # noqa: F401
    HAS_XLSX = True
except ImportError:
    HAS_XLSX = False
try:
    import py7zr  # noqa: F401
    HAS_7Z = True
except ImportError:
    HAS_7Z = False


def zip_file(path, members, enc="latin-1"):
    """members: {name: text}"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with zipfile.ZipFile(path, "w") as z:
        for name, text in members.items():
            z.writestr(name, text.encode(enc))
    return path


def table(name, db=DB):
    return C.read_table_pd(db, name)


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("tests-empresas").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4").config("spark.hadoop.fs.permissions.umask-mode", "000").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestHelpers(unittest.TestCase):
    def test_cgu_dates_and_headers(self):
        from datetime import date
        self.assertEqual(cgu.candidate_dates(2, date(2026, 10, 23))[:4], ["20261021", "20261022", "20261023", "20261020"])
        self.assertEqual(cgu.norm_header("RAZÃO SOCIAL – CADASTRO  RECEITA"), cgu.norm_header("RAZÃO SOCIAL - CADASTRO RECEITA"))
        self.assertEqual(cgu.table_columns(cgu.DATASETS["cepim"]["csvs"]["CEPIM"])[:3], ["cnpj_entidade", "numero_convenio", "cpf_cnpj_norm"])

    def test_pncp_naming(self):
        self.assertEqual(pncp.camel_to_snake("idContratacaoPNCP"), "id_contratacao_pncp")
        self.assertEqual(pncp.camel_to_snake("numeroControlePNCPCompra"), "numero_controle_pncp_compra")
        self.assertEqual(pncp.norm_value("2026-09-04T14:10:13"), "2026-09-04 14:10:13")
        self.assertEqual((pncp.norm_value(True), pncp.norm_value(""), pncp.norm_value(3)), ("True", None, "3"))
        self.assertEqual(pncp.csv_months_for(None, [2025])[:2], ["2025-01", "2025-02"])
        with self.assertRaises(ValueError):
            pncp.csv_months_for(None, None)

    def test_cvm_and_caged_names(self):
        self.assertEqual(cvm.statement_key("dfp_cia_aberta_BPA_con_2024.csv", "DFP"), "bpa_con")
        self.assertIsNone(cvm.statement_key("leia-me.txt", "DFP"))
        self.assertEqual(caged.normalize_col(" Competência Mov "), "competencia_mov")
        self.assertEqual(caged.normalize_col("saláriomediano"), "salariomediano")


class TestMasking(Base):
    def test_mask_cpf_col_and_trailing_cpf(self):
        df = self.spark.createDataFrame([("07935099120",), ("079.350.991-20",), ("12800672000160",), ("***.642.455-**",), (None,)], "v string")
        got = [r[0] for r in df.select(C.mask_cpf_col("v")).collect()]
        self.assertEqual(got, ["***350991**", "***.350.991-**", "12800672000160", "***.642.455-**", None])
        df = self.spark.createDataFrame([("JOAO DA SILVA 12345678901",), ("ACME 2000 LTDA",), ("12345678901",)], "v string")
        got = [r[0] for r in df.select(cnpj.mask_trailing_cpf(df["v"])).collect()]
        self.assertEqual(got, ["JOAO DA SILVA ***456789**", "ACME 2000 LTDA", "***456789**"])


class TestCnpj(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        rel = os.path.join(FIX, "rf", "2026-09-14")
        est = ['"11111111";"0001";"11";"1";"ALFA";"02";"20200101";"00";"";"";"20200101";"4711302";"";"RUA";"A";"1";"";"C";"29000000";"ES";"5705";"";"";"";"";"";"";"";"";""',
               '"22222222";"0001";"22";"1";"BETA";"02";"20200101";"00";"";"";"20200101";"4711302";"";"RUA";"B";"2";"";"C";"01000000";"SP";"7107";"";"";"";"";"";"";"";"";""',
               '"33333333";"0002";"33";"2";"GAMA";"08";"20200101";"01";"";"";"20200101";"4711302";"";"RUA";"C";"3";"";"C";"29100000";"ES";"5705";"";"";"";"";"";"";"";"";""']
        emp = ['"11111111";"ALFA COMERCIO LTDA";"2062";"49";"1000,00";"01";""',
               '"22222222";"BETA LTDA";"2062";"49";"10,00";"05";""',
               '"33333333";"MARIA DE SOUZA 12345678901";"2135";"50";"0,00";"01";""']
        soc = ['"11111111";"2";"FULANO DE TAL";"***123456**";"49";"20200101";"";"***000000**";"";"00";"5"',
               '"22222222";"2";"CICLANO";"***654321**";"49";"20200101";"";"***000000**";"";"00";"4"']
        zip_file(os.path.join(rel, "Estabelecimentos1.zip"), {"K3241.K03200Y1.D60912.ESTABELE": "\n".join(est) + "\n"})
        zip_file(os.path.join(rel, "Empresas1.zip"), {"K3241.K03200Y1.D60912.EMPRECSV": "\n".join(emp) + "\n"})
        zip_file(os.path.join(rel, "Socios1.zip"), {"K3241.K03200Y1.D60912.SOCIOCSV": "\n".join(soc) + "\n"})
        zip_file(os.path.join(rel, "Cnaes.zip"), {"F.K03200$Z.D60912.CNAECSV": '"4711302";"Comércio varejista"\n'})
        cls.base = "file://" + os.path.join(FIX, "rf")

    def run_cnpj(self, **kw):
        return cnpj.main(self.spark, reference_date="2026-09-14", base_url=self.base, target_db=DB, **kw)

    def test_01_ufs_filter_semi_join_and_mei_mask(self):
        r = self.run_cnpj(tables=["Empresas", "Socios", "Estabelecimentos", "Cnaes"], parts=[1], ufs=["ES"])
        self.assertTrue(r["success"], r)
        self.assertEqual(r["tabelas"][0], "Estabelecimentos")  # loaded first: the other tables filter on it
        e = table("cnpj_estabelecimentos")
        self.assertEqual(sorted(e["cnpj_basico"]), ["11111111", "33333333"])
        self.assertEqual(len(e.columns), len(cnpj.TABLE_COLUMNS["Estabelecimentos"]) + 1)
        self.assertTrue(e["complemento"].isna().all())  # '' → NULL
        m = table("cnpj_empresas").set_index("cnpj_basico")
        self.assertEqual(sorted(m.index), ["11111111", "33333333"])
        self.assertEqual(m.loc["33333333", "razao_social"], "MARIA DE SOUZA ***456789**")
        self.assertEqual(list(table("cnpj_socios")["cnpj_basico"]), ["11111111"])
        self.assertEqual(table("cnpj_cnaes").iloc[0]["descricao"], "Comércio varejista")  # latin-1 decoded

    def test_02_failed_part_keeps_previous_snapshot(self):
        self.run_cnpj(tables=["Estabelecimentos"], parts=[1])
        r = self.run_cnpj(tables=["Estabelecimentos"], parts=[1, 2])  # part 2 does not exist
        self.assertFalse(r["success"])
        self.assertEqual(len(table("cnpj_estabelecimentos")), 3)
        self.assertFalse(C.fs().exists(C.table_path(DB, "cnpj_estabelecimentos") + ".__stage"))

    def test_03_quota_guard_without_filter(self):
        saved = (C.STORAGE, C.QUOTA_GIB, C.bucket_usage_bytes, cnpj.file_sizes)
        try:
            C.STORAGE = "s3"  # makes the guard active; nothing reaches S3 because it refuses before writing
            C.QUOTA_GIB = 0.5
            C.bucket_usage_bytes = lambda: 0
            cnpj.file_sizes = lambda url: {"Estabelecimentos1.zip": 1 << 30}
            with self.assertRaises(RuntimeError) as cm:
                cnpj.main(self.spark, reference_date="2026-09-14", base_url=self.base, target_db=DB, tables=["Estabelecimentos"], parts=[1])
            self.assertIn("cota", str(cm.exception))
        finally:
            C.STORAGE, C.QUOTA_GIB, C.bucket_usage_bytes, cnpj.file_sizes = saved


class TestCvm(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dre = ("CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;CD_CONTA;DS_CONTA;VL_CONTA\n"
               "33.000.167/0001-01;2025-12-31;1;PETRÓLEO BRASILEIRO S.A.;9512;3.01;Receita;100,5\n")
        zip_file(os.path.join(FIX, "cvm", "DFP", "DADOS", "dfp_cia_aberta_2025.zip"),
                 {"dfp_cia_aberta_DRE_con_2025.csv": dre, "dfp_cia_aberta_semchave_2025.csv": "A;B\n1;2\n"})
        zip_file(os.path.join(FIX, "cvm", "DFP", "DADOS", "dfp_cia_aberta_2024.zip"),
                 {"dfp_cia_aberta_DRE_con_2024.csv": dre.replace("2025-12-31", "2024-12-31").replace(";VL_CONTA", ";VL_CONTA;EXTRA") + ""})
        cls.base = "file://" + os.path.join(FIX, "cvm")

    def test_tables_per_statement_and_years_union(self):
        r = cvm.main(self.spark, years=[2024, 2025], doc_types=["DFP"], target_db=DB, base_url=self.base)
        self.assertTrue(r["success"], r)
        t = table("cvm_dfp_dre_con")
        self.assertEqual(sorted(t["dt_refer"]), ["2024-12-31", "2025-12-31"])
        self.assertIn("denom_cia", t.columns)
        self.assertEqual(t["denom_cia"].iloc[0], "PETRÓLEO BRASILEIRO S.A.")
        self.assertFalse(C.table_exists(DB, "cvm_dfp_semchave"))  # no cnpj_cia → skipped
        r = cvm.main(self.spark, years=[2025], doc_types=["DFP"], target_db=DB, base_url=self.base)
        self.assertEqual(list(table("cvm_dfp_dre_con")["dt_refer"]), ["2025-12-31"])  # snapshot of the run


class TestPgfn(Base):
    HDR = "CPF_CNPJ;TIPO_PESSOA;TIPO_DEVEDOR;NOME_DEVEDOR;UF_DEVEDOR;NUMERO_INSCRICAO;VALOR_CONSOLIDADO\n"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        q = os.path.join(FIX, "pgfn", "2026_trimestre_03")
        zip_file(os.path.join(q, "Dados_abertos_FGTS.zip"), {
            "arquivo_lai_FGTS_1_202609.csv": cls.HDR + "11.111.111/0001-11;Pessoa jurídica;PRINCIPAL;ALFA;ES;1;10,00\n",
            "arquivo_lai_FGTS_2_202609.csv": cls.HDR + "XXX853.607XX;Pessoa física;PRINCIPAL;JOSE;SP;2;5,00\n"})
        zip_file(os.path.join(q, "Dados_abertos_Previdenciario.zip"), {
            "arquivo_lai_PREV_1_202609.csv": cls.HDR + "22.222.222/0001-22;Pessoa jurídica;PRINCIPAL;BETA;ES;3;7,00\n"})
        cls.base = "file://" + os.path.join(FIX, "pgfn")

    def test_snapshot_with_tipo_and_ufs(self):
        r = pgfn.main(self.spark, tipos=["fgts", "previdenciario"], quarter="2026_trimestre_03", target_db=DB, base_url=self.base)
        self.assertTrue(r["success"], r)
        t = table("pgfn_devedores")
        self.assertEqual(sorted(t["tipo_divida"]), ["fgts", "fgts", "previdenciario"])
        r = pgfn.main(self.spark, tipos=["fgts", "previdenciario"], quarter="2026_trimestre_03", ufs=["es"], target_db=DB, base_url=self.base)
        self.assertEqual(sorted(table("pgfn_devedores")["UF_DEVEDOR"]), ["ES", "ES"])

    def test_failed_type_keeps_snapshot(self):
        pgfn.main(self.spark, tipos=["fgts"], quarter="2026_trimestre_03", target_db=DB + "_f", base_url=self.base)
        r = pgfn.main(self.spark, tipos=["fgts", "nao_previdenciario"], quarter="2026_trimestre_03", target_db=DB + "_f", base_url=self.base)
        self.assertFalse(r["success"])
        self.assertEqual(len(table("pgfn_devedores", DB + "_f")), 2)


class TestCgu(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        spec = cgu.DATASETS["ceis"]["csvs"]["CEIS"]
        hdr = ";".join('"%s"' % h for h, _ in spec["source_columns"])
        n = len(spec["source_columns"])

        def row(cod, tipo, doc, nome, fim=""):
            vals = ["CEIS", cod, tipo, doc, nome] + [""] * (n - 5)
            vals[[c for _, c in spec["source_columns"]].index("data_final_sancao")] = fim
            return ";".join('"%s"' % v for v in vals)

        cls.base = "file://" + os.path.join(FIX, "cgu")
        for d, rows in (("20261021", [row("1", "J", "12.800.672/0001-60", "ACME"), row("2", "F", "079.350.991-20", "JOSE")]),
                        ("20261022", [row("1", "J", "12.800.672/0001-60", "ACME", "31/12/2030"), row("3", "J", "bad", "X"),
                                      row("", "J", "1", "SEM PK")])):
            zip_file(os.path.join(FIX, "cgu", "ceis", "%s_CEIS.zip" % d), {"%s_CEIS.csv" % d: hdr + "\n" + "\n".join(rows) + "\n"}, "cp1252")
        zip_file(os.path.join(FIX, "cgu", "cnep", "20261022_CNEP.zip"), {"20261022_CNEP.csv": "A;B\n1;2\n"}, "cp1252")

    def test_upsert_derived_columns_and_masking(self):
        r = cgu.main(self.spark, cadastros=["ceis"], reference_date="20261021", target_db=DB, base_url=self.base)
        self.assertTrue(r["success"], r)
        t = table("cgu_ceis").set_index("codigo_da_sancao")
        self.assertEqual((t.loc["1", "cpf_cnpj_norm"], t.loc["1", "cnpj_basico"]), ("12800672000160", "12800672"))
        self.assertEqual((t.loc["2", "cpf_ou_cnpj_do_sancionado"], t.loc["2", "cpf_cnpj_norm"]), ("***.350.991-**", "***350991**"))
        self.assertIsNone(t.loc["2", "cnpj_basico"])
        r = cgu.main(self.spark, cadastros=["ceis"], reference_date="20261022", target_db=DB, base_url=self.base)
        tb = r["results"][0]["tables"][0]
        self.assertEqual((tb["rows_read"], tb["rows_skipped_null_pk"], tb["sr_rows"], tb["sr_rows_in_snapshot"]), (3, 1, 3, 2))
        t = table("cgu_ceis").set_index("codigo_da_sancao")
        self.assertEqual(t.loc["1", "data_final_sancao"], "31/12/2030")  # upserted
        self.assertEqual(t.loc["2", "data_referencia"], "2026-10-21")    # not in the new snapshot: kept with its old date
        self.assertIsNone(t.loc["3", "cpf_cnpj_norm"])

    def test_open_cpf_option_and_header_change(self):
        cgu.main(self.spark, cadastros=["ceis"], reference_date="20261021", manter_cpf_aberto=True, target_db=DB + "_o", base_url=self.base)
        self.assertEqual(table("cgu_ceis", DB + "_o").set_index("codigo_da_sancao").loc["2", "cpf_cnpj_norm"], "07935099120")
        r = cgu.main(self.spark, cadastros=["cnep"], reference_date="20261022", target_db=DB, base_url=self.base)
        self.assertFalse(r["success"])
        self.assertIn("header changed", r["results"][0]["error"])
        self.assertFalse(C.table_exists(DB, "cgu_cnep"))

    def test_dry_run_needs_no_spark(self):
        r = cgu.main(None, cadastros=["ceis"], reference_date="20261022", dry_run=True, target_db="t_none", base_url=self.base)
        self.assertEqual(r["results"][0]["tables"][0]["rows_read"], 3)
        self.assertFalse(C.table_exists("t_none", "cgu_ceis"))


class TestPncp(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        spec = pncp.DATASETS["resultados"]
        cols = spec["csv_columns"]
        hdr = ";".join('"%s"' % (c.upper() if c == "cod_resultado_item" else c.replace("_pncp_compra", "_PNCP_compra")) for c in cols)

        def row(ctrl, item, seq, ni, tipo):
            v = dict.fromkeys(cols, "")
            v.update(numero_controle_pncp_compra=ctrl, numero_item_pncp=item, sequencial_resultado=seq, ni_fornecedor=ni, tipo_pessoa=tipo,
                     motivo_cancelamento="linha 1\nlinha 2")
            return ";".join('"%s"' % v[c] for c in cols)

        text = "﻿" + hdr + "\n" + "\n".join([row("A-1/2026", "1", "1", "22182241000131", "PJ"), row("A-1/2026", "2", "1", "06907546639", "PF"),
                                                  row("", "3", "1", "x", "PJ")]) + "\n"
        zip_file(os.path.join(FIX, "pncp", "2026", "08", "item_resultado_2026_08.csv.zip"), {"item_resultado_2026_08.csv": text}, "utf-8")
        cls.base = "file://" + os.path.join(FIX, "pncp")

    def test_csv_month_upsert_and_masking(self):
        r = pncp.main(self.spark, mode="csv", datasets=["resultados"], csv_months=["2026-08", "2026-09"], target_db=DB, csv_base=self.base)
        self.assertTrue(r["success"], r)
        res = r["results"]["resultados"]
        self.assertEqual((res["rows"], res["skipped_null_pk"], len(res["files"])), (2, 1, 1))  # 2026-09 not published
        t = table("comprasgov_pncp_resultados").set_index("numero_item_pncp")
        self.assertEqual(t.loc["2", "ni_fornecedor"], "***075466**")
        self.assertEqual(t.loc["1", "ni_fornecedor"], "22182241000131")
        self.assertEqual(t.loc["1", "motivo_cancelamento"], "linha 1\nlinha 2")  # quoted newline kept (multiLine)
        self.assertEqual(t.loc["1", "origem_carga"], "csv")
        pncp.main(self.spark, mode="csv", datasets=["resultados"], csv_months=["2026-08"], target_db=DB, csv_base=self.base)
        self.assertEqual(len(table("comprasgov_pncp_resultados")), 2)

    def test_api_mode_pages_and_modalidades(self):
        calls = []

        def fake(path, params, api_base=None):
            calls.append(dict(params))
            if "codigoModalidade" in params:
                return {"resultado": [], "totalRegistros": 0, "paginasRestantes": 0}
            if params["pagina"] == 1:
                return {"resultado": [{"numeroControlePNCPCompra": "B-1/2026", "numeroItemPncp": 1, "sequencialResultado": 1,
                                       "niFornecedor": "12345678901", "dataResultadoPncp": "2026-09-04T14:10:13", "campoNovo": 1}],
                        "totalRegistros": 2, "totalPaginas": 2, "paginasRestantes": 1}
            return {"resultado": [{"numeroControlePNCPCompra": "B-1/2026", "numeroItemPncp": 2, "sequencialResultado": 1,
                                   "indicadorSubcontratacao": False}], "paginasRestantes": 0}

        orig = pncp.api_get
        pncp.api_get = fake
        try:
            r = pncp.main(self.spark, mode="api", datasets=["resultados", "contratacoes"], date_start="2026-09-01", date_end="2026-09-02",
                          target_db=DB + "_api")
        finally:
            pncp.api_get = orig
        self.assertTrue(r["success"], r)
        self.assertEqual(r["results"]["resultados"]["rows"], 2)
        self.assertEqual(sorted({c.get("codigoModalidade") for c in calls if "codigoModalidade" in c}), [0, 3, 5, 6, 7])
        t = table("comprasgov_pncp_resultados", DB + "_api").set_index("numero_item_pncp")
        self.assertEqual((t.loc["1", "data_resultado_pncp"], t.loc["1", "ni_fornecedor"], t.loc["2", "indicador_subcontratacao"]),
                         ("2026-09-04 14:10:13", "***456789**", "False"))
        self.assertFalse(C.table_exists(DB + "_api", "comprasgov_pncp_contratacoes"))  # nothing returned → no table


@unittest.skipUnless(HAS_XLSX, "openpyxl not installed (rebuild the jupyter image)")
class TestMte(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Cadastro de Empregadores"])
        ws.append(["Atualização periódica de 6 de outubro de 2026. Cadastro atualizado em 10/10/2026."])
        ws.append([])
        hdr = [h for h, _ in mte.SOURCE_COLUMNS]
        ws.append([hdr[0], hdr[1], hdr[2], hdr[3], None, hdr[4], hdr[5], hdr[6], hdr[7], hdr[8], hdr[9]])  # merged cell gap
        for i in range(120):
            doc = "12.345.678/0001-%02d" % (i % 100) if i % 2 else "123.456.789-%02d" % (i % 100)
            ws.append([i + 1, 2024, "MG", "EMPREGADOR %d%s" % (i, " (*1)" if i == 3 else ""), None, doc, "Fazenda %d" % i, 3, "0151-2/01",
                       "01/02/2025", "01/03/2025"])
        ws.append(["Página 1 de 1"])
        buf = io.BytesIO()
        wb.save(buf)
        p = os.path.join(FIX, "mte.xlsx")
        os.makedirs(FIX, exist_ok=True)
        with open(p, "wb") as f:
            f.write(buf.getvalue())
        cls.url = "file://" + p

    def test_parse_mask_and_upsert(self):
        r = mte.main(self.spark, target_db=DB, url=self.url)
        self.assertEqual((r["edition"], r["rows_parsed"], r["table_rows_after"]), ("2026-10-10", 120, 120))
        t = table("mte_cadastro_empregadores")
        pf = t[t["tipo_pessoa"] == "F"].iloc[0]
        self.assertEqual((pf["cnpj_cpf"], pf["cpf_cnpj_norm"], pf["cpf_mascarado_rf"]), ("***456789**",) * 3)
        self.assertFalse(t.astype(str).apply(lambda c: c.str.contains(r"123\.456\.789|12345678900")).any().any(), "open CPF stored")
        pj = t[t["tipo_pessoa"] == "J"].iloc[0]
        self.assertEqual(len(pj["cnpj_basico"]), 8)
        self.assertEqual(t["nota_rodape"].dropna().tolist(), ["(*1)"])
        self.assertEqual(set(t["edicao_periodica"]), {"2026-10-06"})
        mte.main(self.spark, target_db=DB, url=self.url)
        self.assertEqual(len(table("mte_cadastro_empregadores")), 120)  # keys stable → upsert, no duplicates


@unittest.skipUnless(HAS_7Z, "py7zr not installed (rebuild the jupyter image)")
class TestCaged(Base):
    def make(self, comp, n):
        import py7zr
        d = os.path.join(FIX, "caged")
        os.makedirs(d, exist_ok=True)
        txt = os.path.join(d, "CAGEDMOV%s.txt" % comp)
        with open(txt, "w", encoding="utf-8") as f:
            f.write("competênciamov;região;uf;município;saldomovimentação;salário\n")
            for i in range(n):
                f.write("%s;3;31;310620;%d;1234,56\n" % (comp, 1 if i % 2 else -1))
        arc = os.path.join(d, "CAGEDMOV%s.7z" % comp)
        with py7zr.SevenZipFile(arc, "w") as z:
            z.write(txt, os.path.basename(txt))
        return arc

    def test_partitions_accumulate_and_reload_replaces(self):
        a, b = self.make("202607", 5), self.make("202608", 3)
        r = caged.main(self.spark, local_files=[a, b], target_db=DB)
        self.assertTrue(r["success"], r)
        self.assertEqual(r["particoes_gravadas"], ["202607", "202608"])
        t = table("caged_mov")
        self.assertEqual(list(t.columns[:5]), ["competenciamov", "regiao", "uf", "municipio", "saldomovimentacao"])
        caged.main(self.spark, local_files=[self.make("202608", 4)], target_db=DB)  # month reloaded, the other kept
        t = table("caged_mov")
        self.assertEqual(t.groupby(t["competencia_arquivo"].astype(str)).size().to_dict(), {"202607": 5, "202608": 4})


class TestFlow(unittest.TestCase):
    def test_summary(self):
        s = flow._resumo("x", {"fonte": "rf_cnpj", "success": True, "files_succeeded": 3, "files_failed": 0, "reference_date": "2026-09-14"})
        self.assertEqual((s["fonte"], s["arquivos_ok"], s["reference_date"]), ("rf_cnpj", 3, "2026-09-14"))
        self.assertIn("erro", flow._resumo("x", {"error": "boom"}))


if __name__ == "__main__":
    unittest.main()

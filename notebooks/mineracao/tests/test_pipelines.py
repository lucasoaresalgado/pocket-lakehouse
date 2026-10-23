# Offline tests: Spark transformations and control logic against small fixtures (file:// URLs), no network.

# Run inside the jupyter container:  cd /notebooks/mineracao && python -m unittest discover -s tests -v

import io
import json
import os
import shutil
import sys
import unittest
import zipfile

os.environ["DATA_ROOT"] = "/data/_test"
os.environ["ALLOW_LOCAL_STORAGE"] = "1"  # local-disk mode is blocked outside the tests
os.environ["STORAGE"] = "file"  # this module tests the logic on local disk; test_s3_storage.py covers MinIO
os.environ["PAUSA_S"] = "0"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from pyspark.sql import SparkSession, functions as F

from pipelines import amb, anp, cfem, common as C, dipem, flow, scm, sigbm

ROOT = os.environ["DATA_ROOT"]
FIX = os.path.join(ROOT, "fixtures")
DB = "t_estudo"


def write(name, text, enc="utf-8"):
    os.makedirs(FIX, exist_ok=True)
    p = os.path.join(FIX, name)
    with open(p, "wb") as f:
        f.write(text.encode(enc))
    return "file://" + p


def table(name):
    return pd.read_parquet(C.table_path(DB, name))


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        shutil.rmtree(ROOT, ignore_errors=True)
        os.makedirs(os.path.join(ROOT, "ref"))
        rows = [("3131703", "MG", "Itabira"), ("1505502", "PA", "Paragominas"), ("4314902", "RS", "Porto Alegre"),
                ("3106200", "MG", "Belo Horizonte")] + [("9%06d" % i, "XX", "FILLER %d" % i) for i in range(5100)]
        pd.DataFrame(rows, columns=["cod_ibge", "uf", "municipio"]).to_parquet(os.path.join(ROOT, "ref", "dim_municipiosIbge.parquet"), index=False)
        cls.spark = (SparkSession.builder.master("local[2]").appName("tests").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4").config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
                     .config("spark.hadoop.fs.permissions.umask-mode", "000").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestHelpers(Base):
    def test_norm_python_and_spark_agree(self):
        vals = ["Minério de Ferro", "  Cobre (Calcopirita) ", "SÃO PAULO", "Ñandú-Ç", None, "", "ABC  123", "Água/Óleo"]
        df = self.spark.createDataFrame([(v,) for v in vals], "v string")
        got = [r[0] for r in df.select(C.norm_col("v")).collect()]
        self.assertEqual(got, [C.norm(v) for v in vals])
        self.assertEqual(C.norm("Minério de Ferro"), "MINERIO DE FERRO")

    def test_parse_num(self):
        cases = {"1.234,56": 1234.56, "1234.56": 1234.56, "R$ 10,5": 10.5, "1.234.567": 1234567.0, "-3,5": -3.5, "": None,
                 "-": None, "abc": None, "1,2,3": None, "1e3": 1000.0, "  7 ": 7.0}
        df = self.spark.createDataFrame([(k,) for k in cases], "v string")
        got = {r["v"]: r["n"] for r in df.select("v", C.parse_num_col("v").alias("n")).collect()}
        for k, v in cases.items():
            self.assertEqual(got[k], v, "parse_num(%r)" % k)

    def test_parse_periodo(self):
        cases = {"2024/03": "2024-03", "2024-3-15": "2024-03", "03/2024": "2024-03", "202403": "2024-03", "15/03/2024": "2024-03",
                 "jan/2024": "2024-01", "Dez 23": "2023-12", "2024-13": None, "": None, "xx": None, "2024-01-01T00:00": "2024-01"}
        df = self.spark.createDataFrame([(k,) for k in cases], "v string")
        got = {r["v"]: r["p"] for r in df.select("v", C.parse_periodo_col("v").alias("p")).collect()}
        for k, v in cases.items():
            self.assertEqual(got[k], v, "parse_periodo(%r)" % k)

    def test_classifica_and_foco_defaults(self):
        foco = C.load_foco("", "anm_cfem", True)
        names = ["MINERIO DE FERRO", "BAUXITA REFRATARIA", "FOSFATO", "NIQUEL", "OURO", "MANGANES", "NIOBIO", "AREIA", "CALCARIO"]
        exp = ["FERRO", "BAUXITA", "FOSFATO", "NIQUEL", "OURO", "MANGANES", "NIOBIO", "OUTROS", "OUTROS"]
        self.assertEqual([C.classifica(foco, n) for n in names], exp)
        df = self.spark.createDataFrame([(n,) for n in names], "n string")
        self.assertEqual([r[0] for r in df.select(C.classifica_col(foco, F.col("n"))).collect()], exp)
        self.assertEqual(foco.substancias, set(C.PRIORIDADE_PADRAO))
        self.assertEqual(C.load_foco("", "x", False).substancias, {"FERRO", "BAUXITA", "FOSFATO", "NIQUEL"})

    def test_header_mapping_and_csv_split(self):
        self.assertEqual(C.detect_delim("a;b;c"), ";")
        self.assertEqual(C.detect_delim("a,b,c"), ",")
        self.assertEqual(C.split_csv('a;"b;c";d', ";"), ["a", "b;c", "d"])
        idx, livres = C.map_header(["Ano", "Mês", "Substância", "Extra"], [("ano", r"^ANO$"), ("mes", r"^MES$"), ("substancia", r"^SUBSTANCIA")])
        self.assertEqual(idx, {"ano": 0, "mes": 1, "substancia": 2})
        self.assertEqual(livres, ["Extra"])

    def test_gzip_response_is_decompressed(self):
        import gzip
        import io
        plain = b"id;nome\n" + b"1;Itabira\n" * 50000
        raw = gzip.compress(plain)

        class Fake(io.BytesIO):
            headers = {"Content-Encoding": "gzip"}

        r = C._GzipResponse(Fake(raw))
        self.assertEqual(r.read(8), b"id;nome\n")
        self.assertEqual(r.read(), plain[8:])
        self.assertEqual(r.read(), b"")

    def test_months(self):
        self.assertEqual(C.add_months("2025-01", -1), "2024-12")
        self.assertEqual(C.add_months("2024-12", 13), "2026-01")
        self.assertEqual(C.to_ym("x", "2025-03-17"), "2025-03")
        with self.assertRaises(ValueError):
            C.to_ym("x", "2025-13")

    def test_anp_link_period(self):
        self.assertEqual(anp.link_periodo("https://x/producao-poco-2025-03.csv"), {"ini": "2025-03", "fim": "2025-03", "nivel": "mes"})
        self.assertEqual(anp.link_periodo("https://x/producao_pocos_2023.zip"), {"ini": "2023-01", "fim": "2023-12", "nivel": "ano"})
        self.assertEqual(anp.link_periodo("https://x/poco-marco-2022.csv")["ini"], "2022-03")
        self.assertIsNone(anp.link_periodo("https://x/leia-me.csv"))

    def test_anp_link_discovery(self):
        html = ('<a href="/a/pocos-2024-01.csv">a</a><a href="/a/pocos-2024.zip/@@download/file">b</a><a href="/a/outro.pdf">c</a>'
                '<a href="/a/sem-periodo-poco.csv">d</a><a href="/a/pocos-2024-01.csv">dup</a>')
        orig = C.http_text
        C.http_text = lambda u: html
        try:
            import re
            d = anp.descobre_links("https://gov.br/anp/pagina", re.compile(r"po[cç]o", re.I))
        finally:
            C.http_text = orig
        self.assertEqual(d["total"], 3)
        self.assertEqual([l["ini"] for l in d["links"]], ["2024-01", "2024-01"])
        self.assertEqual(len(d["sem_periodo"]), 1)

    def test_flow_summary(self):
        self.assertEqual(flow._resumo([{"fonte": "a", "has_more": True}, {"fonte": "a", "status": "ok", "linhas_carregadas": 5, "has_more": False}])["linhas"], 5)
        self.assertIn("erro", flow._resumo({"error": {"message": "boom"}}))
        self.assertEqual(flow._resumo({"fonte": "x", "dry_run": True})["status"], "dry_run")


class TestCfem(Base):
    CSV = (
        "Ano;Mês;Processo;AnoDoProcesso;Tipo_PF_PJ;CPF_CNPJ;Substância;UF;Município;CodigoMunicipio;QuantidadeComercializada;UnidadeDeMedida;ValorRecolhido\n"
        "2025;1;812.827;1976;J;12.345.678/0001-90;Minério de Ferro;MG;Itabira;3131703;1.234,50;t;100,25\n"
        "2025;1;812.827;1976;J;12.345.678/0001-90;Minério de Ferro;MG;Itabira;3131703;765,50;t;50,00\n"
        "2025;1;111.111;1980;F;123.456.789-01;Minério de Ferro;MG;Itabira;3131703;10,00;t;5,00\n"
        "2025;1;222.222;1981;F;987.654.321-00;Minério de Ferro;MG;Itabira;3131703;20,00;t;7,00\n"
        "2025;2;333.333;1990;J;98765432000100;Bauxita;PA;Paragominas;;500,00;t;30,00\n"
        "2025;1;444.444;1990;J;11111111000111;Calcário;MG;Itabira;3131703;1,00;t;1,00\n"
        "2020;1;555.555;1990;J;22222222000122;Ferro;MG;Itabira;3131703;1,00;t;1,00\n"
        "2025;1;666.666;1990;J;33333333000133;Ferro;RS;Porto Alegre;4314902;1,00;t;1,00\n")

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.url = write("cfem.csv", cls.CSV, "windows-1252")

    def run_cfem(self, **kw):
        return cfem.main(self.spark, urls=[self.url], target_db=DB, **kw)

    def test_01_range_load_filters_aggregates_and_masks_pf(self):
        r = self.run_cfem(mode="range", start_date="2025-01", end_date="2025-03")
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["linhas_lidas"], 8)
        self.assertEqual(r["linhas_no_foco"], 5)  # calcário, 2020 and RS are out
        t = table("anm_cfem_arrecadacao").sort_values(["ano_mes", "processo"]).reset_index(drop=True)
        self.assertEqual(len(t), 3)  # PJ (2 source rows → 1), PF (2 source rows → 1 'PF' row), PA
        pj = t[t["processo"] == "812.827/1976"].iloc[0]
        self.assertEqual((pj["substancia"], pj["prioridade"], pj["uf"], pj["cod_ibge"], pj["municipio"]), ("FERRO", "P1", "MG", "3131703", "Itabira"))
        self.assertEqual(pj["titular_doc"], "12345678000190")
        self.assertEqual(float(pj["quantidade"]), 2000.0)
        self.assertEqual(float(pj["valor_recolhido"]), 150.25)
        self.assertEqual(int(pj["n_registros"]), 2)
        pf = t[t["tipo_titular"] == "PF"].iloc[0]
        self.assertEqual((pf["titular_doc"], pf["processo"], int(pf["n_registros"])), ("PF", "PF", 2))
        self.assertEqual(float(pf["valor_recolhido"]), 12.0)
        self.assertFalse(t.astype(str).apply(lambda c: c.str.contains("12345678901|98765432100")).any().any(), "CPF leaked")
        par = t[t["uf"] == "PA"].iloc[0]  # no source code → resolved by (UF, name)
        self.assertEqual((par["cod_ibge"], par["municipio"], par["substancia"], par["prioridade"]), ("1505502", "Paragominas", "BAUXITA", "P2"))
        self.assertEqual(str(par["ano_mes"])[:10], "2025-02-01")

    def test_02_reload_is_idempotent_and_unchanged_file_is_skipped(self):
        self.run_cfem(mode="range", start_date="2025-01", end_date="2025-03")
        self.run_cfem(mode="range", start_date="2025-01", end_date="2025-03")
        self.assertEqual(len(table("anm_cfem_arrecadacao")), 3)
        r = self.run_cfem(mode="incremental", reference_date="2025-03-10")
        self.assertEqual(r["status"], "sem_mudanca")
        r = self.run_cfem(mode="incremental", reference_date="2025-03-10", force=True)
        self.assertEqual(r["status"], "ok")
        self.assertEqual(len(table("anm_cfem_arrecadacao")), 3)

    def test_03_incremental_window_uses_watermark_and_lookback(self):
        self.run_cfem(mode="range", start_date="2025-01", end_date="2025-03")
        # watermark = 2025-02 → window = [min(2025-04-5m..., 2025-03), 2025-04] with lookback 6 → starts at 2024-11
        plano = cfem.main(None, mode="incremental", reference_date="2025-04-10", urls=[self.url], target_db=DB, dry_run=True)
        self.assertEqual(plano["watermark"], "2025-02")
        self.assertEqual(plano["janela"], "2024-11..2025-04")

    def test_04_profile_does_not_write(self):
        shutil.rmtree(C.table_path(DB, "anm_cfem_arrecadacao"), ignore_errors=True)
        r = self.run_cfem(mode="range", start_date="2025-01", end_date="2025-03", profile=True)
        self.assertTrue(r["profile"])
        self.assertEqual(r["linhas_apos_agregacao"], 3)
        self.assertFalse(C.table_exists(DB, "anm_cfem_arrecadacao"))

    def test_05_layout_change_is_reported_and_recorded(self):
        bad = write("cfem_bad.csv", "A;B;C\n1;2;3\n")
        with self.assertRaisesRegex(ValueError, "CFEM layout changed"):
            cfem.main(self.spark, mode="range", start_date="2025-01", end_date="2025-01", urls=[bad], target_db=DB)
        st = C.Ctl(DB).read_state("anm_cfem", "range")
        self.assertEqual(st["ultimo_status"], "erro")


class TestSigbm(Base):
    HDR = ("ID,Nome,Empreendedor,CPF_CNPJ,Nome da Mina,UF,Município,Latitude,Longitude,Categoria de Risco,Dano Potencial Associado,"
           "Nível de Emergência,Situação Operacional,Inserido na PNSB,Tipo de Barragem de Mineração,Método Construtivo da Barragem,"
           "Tipo de Alteamento,Armazena Rejeitos com Cianeto,Minério principal presente no reservatório,Altura máxima atual (m),"
           "Volume atual do reservatório (m3),Existência de população a jusante,Número de pessoas possivelmente afetadas a jusante,"
           "Data de início de operação,Data da finalização da DCE,RT Nome,Extra Col\n")
    ROWS = (
        '1,B1,VALE SA,12.345.678/0001-90,Mina 1,MG,Itabira,"-19°37\'10.000\'\'","-43°13\'30.000\'\'",Baixo,Alto,Nível 0,Ativa,Sim,Rejeito,A montante,Alteamento,Não,Ferro,"50,5","1.000.000,00",Sim,10,01/02/2010,2024-05-06,Fulano,x1\n'
        "2,B2,JOAO,***123456**,Mina 2,PA,Paragominas,-3.5,-47.1,Médio,Médio,Nível 1,Inativa,Sim,Rejeito,Linha de centro,Alteamento,Não,Ouro,10,1000,Não,0,,,Beltrano,\n"
        "3,B3,ACME,98765432000100,Mina 3,MG,Itabira,,,Baixo,Baixo,Nível 0,Ativa,Sim,Água,,,Não,,5,500,Não,0,,,,y3\n"
        "4,B4,ACME,98765432000100,Mina 4,MG,Itabira,,,Baixo,Baixo,Nível 0,Ativa,Sim,Água,,,Não,Calcário,5,500,Não,0,,,,\n"
        "1,DUP,DUP,12345678000190,Mina 1,MG,Itabira,,,Baixo,Baixo,Nível 0,Ativa,Sim,Água,,,Não,Ferro,5,500,Não,0,,,,\n"
        "6,B6,ACME,98765432000100,Mina 6,RS,Porto Alegre,,,Baixo,Baixo,Nível 0,Ativa,Sim,Água,,,Não,Ferro,5,500,Não,0,,,,\n")

    def test_load(self):
        url = write("sigbm.csv", self.HDR + self.ROWS, "windows-1252")
        r = sigbm.main(self.spark, urls=[url], target_db=DB)
        self.assertEqual(r["status"], "ok")
        t = table("anm_sigbm_barragem").set_index("id_barragem")
        self.assertEqual(sorted(t.index), ["1", "2", "3"])  # calcário, RS and the duplicated id are out
        b1 = t.loc["1"]
        self.assertEqual((b1["tipo_empreendedor"], b1["empreendedor_doc"], b1["empreendedor"]), ("PJ", "12345678000190", "VALE SA"))
        self.assertAlmostEqual(float(b1["latitude"]), -19.619444, places=6)
        self.assertAlmostEqual(float(b1["longitude"]), -43.225, places=6)
        self.assertEqual((b1["substancia"], b1["prioridade"], b1["cod_ibge"]), ("FERRO", "P1", "3131703"))
        self.assertEqual(float(b1["altura_atual_m"]), 50.5)
        self.assertEqual(float(b1["volume_atual_m3"]), 1000000.0)
        self.assertEqual(str(b1["dt_inicio_operacao"])[:10], "2010-02-01")
        self.assertEqual(str(b1["dt_finalizacao_dce"])[:10], "2024-05-06")
        attrs = json.loads(b1["atributos"])
        self.assertEqual(attrs.get("Extra Col"), "x1")
        self.assertNotIn("RT Nome", attrs)  # responsável técnico never stored
        b2 = t.loc["2"]  # PF masked: no document, no name
        self.assertEqual((b2["tipo_empreendedor"], b2["empreendedor_doc"]), ("PF", "PF"))
        self.assertTrue(pd.isna(b2["empreendedor"]))
        self.assertAlmostEqual(float(b2["latitude"]), -3.5)
        self.assertTrue(pd.isna(t.loc["3"]["substancia"]))  # no ore informed → kept by UF, substance NULL
        self.assertEqual(r["sem_minerio_informado"], 1)
        self.assertEqual(r["pf"], 1)

    def test_snapshot_replaces_and_empty_focus_preserves_table(self):
        url = write("sigbm.csv", self.HDR + self.ROWS, "windows-1252")
        sigbm.main(self.spark, urls=[url], target_db=DB)
        only_rs = write("sigbm_rs.csv", self.HDR + self.ROWS.splitlines(True)[-1], "windows-1252")
        with self.assertRaisesRegex(RuntimeError, "snapshot not written"):
            sigbm.main(self.spark, urls=[only_rs], target_db=DB, mode="backfill")
        self.assertEqual(len(table("anm_sigbm_barragem")), 3)


class TestDipem(Base):
    CSV = ("CodigoMunicipio;Municipio;UF;Substancia;Ano;ValorAnaliseQuimica;ValorGeologia;ValorSondagem\n"
           "3131703;Itabira;MG;Ferro;2022;1.000,50;,00;200,00\n"
           "3131703;Itabira;MG;Ferro;2022;500,00;,00;,00\n"
           "3131703;Itabira;MG;-;2022;999,00;,00;,00\n"
           "3131703;Itabira;MG;Calcario;2022;999,00;,00;,00\n"
           "4314902;Porto Alegre;RS;Ferro;2022;999,00;,00;,00\n"
           "1505502;Paragominas;PA;Bauxita;2021;,00;70,00;,00\n")

    def test_load(self):
        r = dipem.main(self.spark, urls=[write("dipem.csv", self.CSV, "windows-1252")], target_db=DB)
        self.assertEqual(r["status"], "ok")
        t = table("anm_dipem_investimento")
        got = {(int(a), c, ti): float(v) for a, c, ti, v in zip(t["ano"], t["cod_ibge"], t["tipo_investimento"], t["valor_rs"])}
        self.assertEqual(got, {(2022, "3131703", "ANALISE_QUIMICA"): 1500.50, (2022, "3131703", "SONDAGEM"): 200.0, (2021, "1505502", "GEOLOGIA"): 70.0})
        self.assertEqual(float(r["valor_foco_rs"]), 1770.50)
        self.assertEqual(set(t["substancia"]), {"FERRO", "BAUXITA"})


class TestAmb(Base):
    CSV = ("Ano;UF;Município;Substância;Produção Bruta (t);Teor (%);Classe;CPF Responsável\n"
           "2023;MG;Itabira;Ferro;1.000,0;62,5;Minério;12345678901\n"
           "2023;MG;Itabira;Ferro;500,0;;Minério;12345678901\n"
           "2023;SC;Criciúma;Ferro;9,0;1,0;X;1\n"
           "2023;MG;Itabira;Calcário;9,0;1,0;X;1\n"
           "2008;MG;Itabira;Ferro;9,0;1,0;X;1\n")

    def test_long_format(self):
        url = write("Producao_Bruta.csv", self.CSV, "windows-1252")
        r = amb.main(self.spark, urls=[url], target_db=DB, reference_date="2024-06-01")
        self.assertEqual(r["status"], "ok")
        t = table("anm_amb_producao")
        self.assertEqual(set(t["tipo"]), {"BRUTA"})
        by = {m: (float(v), u) for m, v, u in zip(t["metrica"], t["valor"], t["unidade"])}
        self.assertEqual(by, {"Produção Bruta (t)": (1500.0, "t"), "Teor (%)": (62.5, "%")})
        self.assertEqual(set(t["atributos"]), {'{"Classe":"Minério"}'})
        self.assertEqual(set(t["cod_ibge"]), {"3131703"})
        self.assertFalse(t.astype(str).apply(lambda c: c.str.contains("12345678901")).any().any(), "CPF column leaked")


class TestAnp(Base):
    CSV = ("Ano;Mês;Estado;Bacia;Nome Poço ANP;Campo;Operador;Número do Contrato;Instalação Destino;Tipo de Instalação;"
           "Petróleo (bbl/dia);Gás Associado (Mm3/dia);Água (bbl/dia);Tempo de Produção (hs por mês)\n"
           "2025;1;Rio de Janeiro;Campos;7-ABC-1-RJS;MARLIM;PETROBRAS;C1;FPSO A;FPSO;1.000,5;10,0;5,0;720\n"
           "2025;1;Rio de Janeiro;Campos;7-ABC-1-RJS;MARLIM;PETROBRAS;C1;FPSO A;FPSO;500,0;5,0;;300\n"
           "2025;1;Rio de Janeiro;Campos;7-DEF-2-RJS;MARLIM;PETROBRAS;C1;FPSO A;FPSO;200,0;2,0;1,0;700\n"
           "2025;1;Amazonas;Solimões;1-XYZ-1-AM;URUCU;PETROBRAS;C2;UTGCA;Terra;50,0;1,0;1,0;700\n"
           "2025;2;Rio de Janeiro;Campos;7-ABC-1-RJS;MARLIM;PETROBRAS;C1;FPSO A;FPSO;9,0;9,0;9,0;9\n")

    def test_load_with_view_and_backfill_cursor(self):
        url = write("anp_202501.csv", self.CSV, "windows-1252")
        r = anp.main(self.spark, mode="range", start_date="2025-01", end_date="2025-01", urls=[url], target_db=DB)
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["descartes"], {"fora_do_foco": 1, "fora_do_lote": 1})
        t = table("anp_producao_poco").set_index("poco")
        self.assertEqual(sorted(t.index), ["7-ABC-1-RJS", "7-DEF-2-RJS"])
        a = t.loc["7-ABC-1-RJS"]
        self.assertEqual((float(a["petroleo"]), float(a["gas_associado"]), float(a["agua"]), float(a["tempo_producao"])), (1500.5, 15.0, 5.0, 720.0))
        self.assertEqual((a["uf"], a["bacia"], a["instalacao"], str(a["ano_mes"])[:10]), ("RJ", "Campos", "FPSO A", "2025-01-01"))
        v = table("anp_producao_instalacao")
        self.assertEqual((len(v), int(v.iloc[0]["n_pocos"]), float(v.iloc[0]["petroleo"])), (1, 2, 1700.5))
        # backfill cursor: 12 months per run, has_more while the window is not exhausted
        p = anp.main(None, mode="backfill", start_date="2023-01", end_date="2025-06", urls=[url], target_db=DB, dry_run=True)
        self.assertEqual((p["lote"], p["has_more"]), ("2023-01..2023-12", True))
        C.Ctl(DB).write_state("anp_poco", "backfill", "2023-12", "2023-01", "2025-06", "ok", {})
        p = anp.main(None, mode="backfill", start_date="2023-01", end_date="2025-06", urls=[url], target_db=DB, dry_run=True)
        self.assertEqual((p["lote"], p["has_more"]), ("2024-01..2024-12", True))
        C.Ctl(DB).write_state("anp_poco", "backfill", "2024-12", "2023-01", "2025-06", "ok", {})
        p = anp.main(None, mode="backfill", start_date="2023-01", end_date="2025-06", urls=[url], target_db=DB, dry_run=True)
        self.assertEqual((p["lote"], p["has_more"]), ("2025-01..2025-06", False))


class TestAnpPresal(Base):
    HDR = "Ano;Mês;Estado;Bacia;Nome Poço ANP;Campo;Operador;Instalação Destino;Petróleo (bbl/dia)\n"
    MAR = HDR + "2025;1;Rio de Janeiro;Santos;P-PRESAL-1;LULA;PETROBRAS;FPSO X;1.000,0\n2025;1;Rio de Janeiro;Campos;P-MAR-1;MARLIM;PETROBRAS;FPSO Y;200,0\n"
    PRESAL = HDR + "2025;1;Rio de Janeiro;Santos;P-PRESAL-1;LULA;PETROBRAS;FPSO X;1.000,0\n"

    def _zip(self):
        p = os.path.join(FIX, "anp_2025_01.zip")
        os.makedirs(FIX, exist_ok=True)
        with zipfile.ZipFile(p, "w") as z:
            z.writestr("2025_01_producao_Mar.csv", self.MAR.encode("windows-1252"))
            z.writestr("2025_01_producao_Presal.csv", self.PRESAL.encode("windows-1252"))
        return "file://" + p

    def test_presal_subset_is_not_counted_twice(self):
        r = anp.main(self.spark, mode="range", start_date="2025-01", end_date="2025-01", urls=[self._zip()], target_db="t_presal")
        t = pd.read_parquet(C.table_path("t_presal", "anp_producao_poco")).set_index("poco")
        self.assertEqual(float(t.loc["P-PRESAL-1"]["petroleo"]), 1000.0)  # not 2000
        self.assertEqual(r["descartes"].get("presal_ignorado"), 1)

    def test_presal_double_count_available(self):
        anp.main(self.spark, mode="range", start_date="2025-01", end_date="2025-01", urls=[self._zip()], target_db="t_presal2", excluir_presal=False)
        t = pd.read_parquet(C.table_path("t_presal2", "anp_producao_poco")).set_index("poco")
        self.assertEqual(float(t.loc["P-PRESAL-1"]["petroleo"]), 2000.0)  # the pre-fix behaviour, double-counting pre-salt


class TestScm(Base):
    FILES = {
        "Processo": "DSProcesso;NRProcesso;NRAnoProcesso;NRNUP;BTAtivo;IDFaseProcesso;IDTipoRequerimento;IDUnidadeAdministrativaRegional;DTProtocolo;DTPrioridade;QTAreaHA\n"
                    "812.827/1976;812827;1976;NUP1;S;1;1;1;1976-05-01 00:00:00;1976-05-01;1.000,50\n"
                    "900.000/2000;900000;2000;NUP2;S;1;1;1;2000-01-01;;10\n"
                    "100.000/1990;100000;1990;NUP3;N;1;1;1;1990-01-01;;10\n",
        "ProcessoSubstancia": "DSProcesso;IDSubstancia;IDTipoUsoSubstancia;IDMotivoEncerramentoSubstancia;DTInicioVigencia;DTFimVigencia\n"
                              "812.827/1976;1;1;;1976-05-01;\n812.827/1976;3;1;;1976-05-01;\n900.000/2000;2;1;;2000-01-01;\n100.000/1990;3;1;;1990-01-01;\n",
        "ProcessoMunicipio": "DSProcesso;IDMunicipio\n812.827/1976;3131703\n900.000/2000;4314902\n100.000/1990;3131703\n",
        "ProcessoPessoa": "DSProcesso;IDPessoa;IDTipoRelacao;DTInicioVigencia;DTFimVigencia\n812.827/1976;10;1;1976-05-01;\n"
                          "812.827/1976;11;1;1990-01-01;\n812.827/1976;12;3;1990-01-01;\n900.000/2000;10;1;2000-01-01;\n",
        "ProcessoTitulo": "DSProcesso;NRTitulo;IDDocumentoLegal;IDTipoDocumentoLegal;IDSituacaoDocumentoLegal;DTPublicacao;DTVencimento\n"
                          "812.827/1976;T1;1;1;1;1980-01-01;2030-01-01\n900.000/2000;T2;1;1;1;2001-01-01;\n",
        "ProcessoEvento": "DSProcesso;IDEvento;DTEvento;OBEvento;DSPublicacaoDOU\n812.827/1976;1;1976-05-02;obs a;Titular FULANO DE TAL\n"
                          "812.827/1976;1;1976-05-02;obs a;Titular FULANO DE TAL\n812.827/1976;2;1977-01-01;obs b;Outro texto\n"
                          "900.000/2000;1;2000-02-02;x;y\n",
        "Pessoa": "IDPessoa;NMPessoa;NRCPFCNPJ;TPPessoa\n10;VALE SA;12.345.678/0001-90;J\n11;FULANO DE TAL;123.456.789-01;F\n12;BELTRANO;***456789**;F\n",
        "Municipio": "IDMunicipio;NMMunicipio;SGUF\n3131703;ITABIRA;MG\n4314902;PORTO ALEGRE;RS\n",
        "Substancia": "IDSubstancia;NMSubstancia\n1;MINÉRIO DE FERRO\n2;OURO\n3;CALCÁRIO\n",
        "Evento": "IDEvento;NMEvento\n1;REQUERIMENTO\n2;ALVARÁ\n",
        "FaseProcesso": "IDFaseProcesso;NMFase\n1;Concessão de Lavra\n",
        "TipoRequerimento": "ID;Nome\n1;Requerimento de Lavra\n",
        "UnidadeAdministrativaRegional": "ID;Nome\n1;MG\n",
        "TipoUsoSubstancia": "ID;Nome\n1;Principal\n",
        "MotivoEncerramentoSubstancia": "ID;Nome\n1;Nenhum\n",
        "DocumentoLegal": "ID;Nome\n1;Portaria de Lavra\n",
        "TipoDocumentoLegal": "ID;Nome\n1;Portaria\n",
        "SituacaoDocumentoLegal": "ID;Nome\n1;Vigente\n",
        "TipoRelacao": "ID;Nome\n1;Titular/Requerente\n3;Responsável Técnico\n",
    }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.makedirs(FIX, exist_ok=True)
        cls.zip_path = os.path.join(FIX, "microdados-scm.zip")
        with zipfile.ZipFile(cls.zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            for name, text in cls.FILES.items():
                z.writestr("microdados/%s.txt" % name, text.encode("utf-8"))
        cls.url = "file://" + cls.zip_path

    def test_load_focus_lgpd_and_snapshot(self):
        r = scm.main(self.spark, urls=[self.url], target_db=DB)
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["processos_no_foco"], 1)  # ouro/RS and calcário are out
        p = table("anm_scm_processo")
        self.assertEqual(list(p["processo"]), ["812.827/1976"])
        row = p.iloc[0]
        self.assertEqual((row["processo_chave"], row["substancias_foco"], row["ufs"], row["fase"], bool(row["ativo"])),
                         ("812827/1976", "FERRO", "MG", "Concessão de Lavra", True))
        self.assertEqual(float(row["area_ha"]), 1000.5)
        self.assertEqual(str(row["dt_protocolo"])[:10], "1976-05-01")
        s = table("anm_scm_substancia")  # ALL substances of a focus process are kept, the non-focus one as OUTROS
        self.assertEqual(sorted(s["substancia"]), ["FERRO", "OUTROS"])
        self.assertEqual(sorted(table("anm_scm_municipio")["municipio"]), ["Itabira"])
        pe = table("anm_scm_pessoa")
        self.assertEqual(len(pe), 3)
        pj = pe[pe["tipo_pessoa"] == "PJ"].iloc[0]
        self.assertEqual((pj["documento"], pj["nome"], pj["tipo_relacao"]), ("12345678000190", "VALE SA", "Titular/Requerente"))
        pf = pe[pe["tipo_pessoa"] == "PF"]
        self.assertEqual(sorted(zip(pf["tipo_relacao"], pf["n_pessoas"])), [("Responsável Técnico", 1), ("Titular/Requerente", 1)])
        self.assertTrue(pf["nome"].isna().all() and (pf["documento"] == "PF").all())
        self.assertFalse(pe.astype(str).apply(lambda c: c.str.contains("FULANO|BELTRANO|12345678901")).any().any(), "PF data leaked")
        ev = table("anm_scm_evento")
        self.assertEqual(len(ev), 2)  # exact duplicate collapsed
        self.assertTrue(ev["publicacao_dou"].isna().all(), "DOU text must be dropped when a PF is titular")
        self.assertEqual(sorted(ev["evento"]), ["ALVARÁ", "REQUERIMENTO"])
        self.assertEqual(len(table("anm_scm_titulo")), 1)
        again = scm.main(self.spark, urls=[self.url], target_db=DB)
        self.assertEqual(again["status"], "sem_mudanca")

    def test_remote_zip_listing_reads_central_directory(self):
        nomes = scm.zip_listing_remoto(self.url, os.path.getsize(self.zip_path))
        self.assertEqual(sorted(nomes), sorted(n + ".txt" for n in self.FILES))
        dry = scm.main(None, urls=[self.url], target_db=DB, dry_run=True)
        self.assertTrue(dry["verificacao_fonte"][0]["layout_ok"], dry["verificacao_fonte"])

    def test_missing_table_in_zip(self):
        zp = os.path.join(FIX, "bad.zip")
        with zipfile.ZipFile(zp, "w") as z:
            z.writestr("Processo.txt", "DSProcesso\n")
        with self.assertRaisesRegex(ValueError, "SCM layout changed"):
            scm.main(self.spark, urls=["file://" + zp], target_db="t_bad", mode="backfill")


if __name__ == "__main__":
    unittest.main()

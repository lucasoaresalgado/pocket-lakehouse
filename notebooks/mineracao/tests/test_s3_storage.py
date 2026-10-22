# Integration test against the real MinIO (bucket `lake`, throw-away prefix that is deleted afterwards).

import os
import sys
import unittest
import uuid

os.environ["STORAGE"] = "s3"
os.environ["S3_BUCKET"] = "lake"
os.environ["DATA_ROOT"] = "/data/_test_s3"
os.environ["PAUSA_S"] = "0"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from pyspark.sql import SparkSession

from pipelines import cfem, common as C, sigbm

DB = "_test_" + uuid.uuid4().hex[:8]
FIX = os.path.join(os.environ["DATA_ROOT"], "fixtures")

CFEM = ("Ano;Mês;Processo;AnoDoProcesso;Tipo_PF_PJ;CPF_CNPJ;Substância;UF;Município;CodigoMunicipio;QuantidadeComercializada;UnidadeDeMedida;ValorRecolhido\n"
        "2025;1;812.827;1976;J;12345678000190;Minério de Ferro;MG;Itabira;3131703;1.000,00;t;100,00\n"
        "2025;1;111.111;1980;F;12345678901;Minério de Ferro;MG;Itabira;3131703;10,00;t;5,00\n")
SIGBM = ("ID,Nome,Empreendedor,CPF_CNPJ,Nome da Mina,UF,Município,Latitude,Longitude,Minério principal presente no reservatório\n"
         "1,B1,VALE,12345678000190,M1,MG,Itabira,-19.5,-43.2,Ferro\n")


def write(name, text):
    os.makedirs(FIX, exist_ok=True)
    p = os.path.join(FIX, name)
    with open(p, "wb") as f:
        f.write(text.encode("windows-1252"))
    return "file://" + p


def minio_up():
    try:
        C.fs().ls(C.S3_BUCKET)
        return True
    except Exception:
        return False


@unittest.skipUnless(minio_up(), "MinIO not reachable")
class TestMinioStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.makedirs(os.path.join(os.environ["DATA_ROOT"], "ref"), exist_ok=True)
        rows = [("3131703", "MG", "Itabira")] + [("9%06d" % i, "XX", "FILLER %d" % i) for i in range(5100)]
        pd.DataFrame(rows, columns=["cod_ibge", "uf", "municipio"]).to_parquet(os.path.join(os.environ["DATA_ROOT"], "ref", "dim_municipiosIbge.parquet"), index=False)
        cls.spark = (SparkSession.builder.master("local[2]").appName("s3-tests").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4").getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        C._rm("%s/%s" % (C.S3_BUCKET, DB))
        for leftover in C.fs().ls(C.S3_BUCKET, detail=False):
            assert not leftover.endswith(DB), "test data left behind in MinIO"

    def test_upsert_ctl_and_snapshot_live_in_minio(self):
        url = write("cfem.csv", CFEM)
        r = cfem.main(self.spark, mode="range", start_date="2025-01", end_date="2025-01", urls=[url], target_db=DB)
        self.assertEqual((r["status"], r["linhas_carregadas"]), ("ok", 2))
        self.assertTrue(C.table_exists(DB, "anm_cfem_arrecadacao"))
        self.assertFalse(os.path.exists(os.path.join(os.environ["DATA_ROOT"], DB)), "nothing may be written to local disk")
        self.assertEqual(len(C.read_table_pd(DB, "anm_cfem_arrecadacao")), 2)
        self.assertEqual(C.watermark_ym(DB, "anm_cfem_arrecadacao"), "2025-01")
        cfem.main(self.spark, mode="range", start_date="2025-01", end_date="2025-01", urls=[url], target_db=DB)  # upsert twice
        self.assertEqual(len(C.read_table_pd(DB, "anm_cfem_arrecadacao")), 2)
        self.assertEqual(C.Ctl(DB).read_state("anm_cfem", "range")["ultimo_status"], "ok")
        self.assertEqual(cfem.main(self.spark, mode="incremental", reference_date="2025-02-01", urls=[url], target_db=DB)["janela"], "2024-09..2025-02")
        # staging leftovers must be gone
        self.assertFalse([f for f in C.fs().ls("%s/%s" % (C.S3_BUCKET, DB), detail=False) if "__" in f])
        s = sigbm.main(self.spark, urls=[write("sigbm.csv", SIGBM)], target_db=DB)
        self.assertEqual(s["linhas_carregadas"], 1)
        self.assertEqual(len(C.read_table_pd(DB, "anm_sigbm_barragem")), 1)


if __name__ == "__main__":
    unittest.main()

# Offline tests: GRIB2 decoding, window planning and the three pipelines against synthetic fixtures (file:// / mocks).

# Run inside the jupyter container:  cd /notebooks/clima && python3 -m unittest tests.test_pipelines

import os
import shutil
import struct
import sys
import unittest

os.environ["DATA_ROOT"] = "/data/_test_clima"
os.environ["ALLOW_LOCAL_STORAGE"] = "1"  # local-disk mode is blocked outside the tests
os.environ["STORAGE"] = "file"
os.environ["PAUSA_S"] = "0"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
from pyspark.sql import SparkSession

from pipelines import flow, grib2, ibge, janela as J, merge, power
from lakehouse import core as C

ROOT = os.environ["DATA_ROOT"]
FIX = os.path.join(ROOT, "fixtures")
DB = "t_clima"
power.PAUSA_S = 0

# three real municipalities + filler rows (the pipelines refuse a municipality table with fewer than 5,000 rows)
REAL = [("3106200", "Belo Horizonte", -19.9102, -43.9266, "1", "31"), ("3131703", "Itabira", -19.6239, -43.2312, "0", "31"),
        ("2607208", "Ipojuca", -8.3982, -35.0634, "0", "26")]


def fixture_ibge():
    os.makedirs(FIX, exist_ok=True)
    mun = ["codigo_ibge,nome,latitude,longitude,capital,codigo_uf,siafi_id,ddd,fuso_horario"]
    mun += ["%s,%s,%s,%s,%s,%s,1,31,America/Sao_Paulo" % r for r in REAL]
    mun += ["3%06d,Filler %d,%.4f,%.4f,0,31,1,31,America/Sao_Paulo" % (i, i, -19 - (i % 50) / 100, -44 - (i // 50) / 100) for i in range(5100)]
    mun += ["3304557,Rio de Janeiro,-22.9129,-43.2003,1,33,1,21,America/Sao_Paulo",
            "3301009,Campos dos Goytacazes,-21.7622,-41.3181,0,33,1,22,America/Sao_Paulo"]
    ufs = [(11, "RO", "Rondônia"), (26, "PE", "Pernambuco"), (31, "MG", "Minas Gerais"), (33, "RJ", "Rio de Janeiro")]
    ufs += [(50 + i, "X%d" % i, "Estado %d" % i) for i in range(23)]
    est = ["codigo_uf,uf,nome,latitude,longitude,regiao"] + ["%d,%s,%s,-10,-50,Sudeste" % u for u in ufs]
    for name, lines in (("municipios", mun), ("estados", est)):
        with open(os.path.join(FIX, name + ".csv"), "w", encoding="utf-8-sig") as f:
            f.write("\n".join(lines) + "\n")
    return "file://" + FIX


# ---------------------------------------------------------------------------
# GRIB2 writer for the fixtures (only what the decoder supports)
# ---------------------------------------------------------------------------

def _bits(values, width):
    return "".join(format(int(v), "0%db" % width) for v in values) if width else ""


def _pack(bitstr):
    bitstr += "0" * (-len(bitstr) % 8)
    return bytes(int(bitstr[i:i + 8], 2) for i in range(0, len(bitstr), 8))


def _sm(v, nbytes):
    """GRIB2 sign-magnitude integer"""
    return ((1 << (8 * nbytes - 1)) | -v if v < 0 else v).to_bytes(nbytes, "big")


def _sec(num, body):
    return struct.pack(">IB", 5 + len(body), num) + body


def grib_message(values, nlat, nlon, lat0, lon0, d=0.1, tpl=0, D=2, bitmap=None, cat=1, num=8, group=3):
    """values: physical values in scan order (south→north rows, west→east), NaN allowed only with a bitmap"""
    vals = np.asarray(values, dtype=float)
    present = vals if bitmap is None else vals[np.asarray(bitmap, dtype=bool)]
    ints = np.round(present * 10 ** D).astype(np.int64)
    lo1 = int(round((lon0 + 360 if lon0 < 0 else lon0) * 1e6))
    s3 = bytearray(67)
    s3[0] = 0
    s3[1:5] = (nlat * nlon).to_bytes(4, "big")
    s3[7:9] = (0).to_bytes(2, "big")  # template 3.0
    s3[25:29] = nlon.to_bytes(4, "big")
    s3[29:33] = nlat.to_bytes(4, "big")
    s3[41:45] = _sm(int(round(lat0 * 1e6)), 4)
    s3[45:49] = _sm(lo1, 4)
    s3[58:62] = int(round(d * 1e6)).to_bytes(4, "big")
    s3[62:66] = int(round(d * 1e6)).to_bytes(4, "big")
    s3[66] = 0x40  # south→north, west→east
    s4 = bytearray(29)
    s4[2:4] = (0).to_bytes(2, "big")
    s4[4], s4[5] = cat, num
    if tpl == 0:
        ref = int(ints.min())
        nb = max(1, int(ints.max() - ref).bit_length())
        s5 = struct.pack(">IHfhhBB", len(ints), 0, float(ref), 0, D, nb, 0)
        data = _pack(_bits(ints - ref, nb))
    else:  # 5.3, second-order spatial differencing, fixed-size groups
        x = ints.copy()
        ival1, ival2 = int(x[0]), int(x[1])
        dd = np.zeros_like(x)
        dd[2:] = x[2:] - 2 * x[1:-1] + x[:-2]
        minsd = int(dd[2:].min())
        dd[2:] -= minsd
        dd[:2] = 0
        groups = [dd[i:i + group] for i in range(0, len(dd), group)]
        grefs = [int(g.min()) for g in groups]
        gws = [int(g.max() - g.min()).bit_length() for g in groups]
        gls = [len(g) for g in groups]
        nb = max(1, max(grefs).bit_length())
        nbw = max(1, max(gws).bit_length())
        nbl = max(1, max(gls).bit_length())
        ospd = 2
        head = _sm(ival1, ospd) + _sm(ival2, ospd) + _sm(minsd, ospd)
        body = (_pack(_bits(grefs, nb)) + _pack(_bits(gws, nbw)) + _pack(_bits(gls, nbl))
                + _pack("".join(_bits(g - r, w) for g, r, w in zip(groups, grefs, gws))))
        data = head + body
        s5 = (struct.pack(">IHfhhBB", len(ints), 3, 0.0, 0, D, nb, 0) + bytes([1, 0]) + b"\xff" * 8
              + struct.pack(">IBBIBIB", len(groups), 0, nbw, 0, 1, gls[-1], nbl) + bytes([2, ospd]))
    s6 = bytes([255]) if bitmap is None else bytes([0]) + _pack("".join("1" if b else "0" for b in bitmap))
    body = (_sec(1, bytes(16)) + _sec(3, bytes(s3)) + _sec(4, bytes(s4)) + _sec(5, s5)
            + _sec(6, s6) + _sec(7, data) + b"7777")
    return b"GRIB" + b"\x00\x00" + bytes([0, 2]) + (16 + len(body)).to_bytes(8, "big") + body


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        shutil.rmtree(ROOT, ignore_errors=True)
        cls.spark = (SparkSession.builder.master("local[2]").appName("tests-clima").config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "4").config("spark.hadoop.fs.permissions.umask-mode", "000").getOrCreate())
        cls.ibge_url = fixture_ibge()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestGrib2(unittest.TestCase):
    def test_simple_packing_with_bitmap(self):
        vals = np.array([0.0, 1.25, np.nan, 3.5, 10.0, np.nan])
        msg = grib2.decode(grib_message(vals, 2, 3, -20.0, -44.0, tpl=0, bitmap=~np.isnan(vals)))[0]
        np.testing.assert_allclose(msg.grid.values, vals, equal_nan=True)
        self.assertEqual((msg.grid.nlat, msg.grid.nlon, msg.grid.dlat, msg.tpl_data), (2, 3, 0.1, 0))
        self.assertAlmostEqual(msg.grid.lon0, -44.0)

    def test_complex_packing_with_second_order_differencing(self):
        rng = np.random.default_rng(7)
        vals = np.round(np.abs(np.cumsum(rng.normal(0, 3, 40))), 2)
        msg = grib2.decode(grib_message(vals, 5, 8, -20.0, -44.0, tpl=3, group=6))[0]
        np.testing.assert_allclose(msg.grid.values, vals, atol=1e-9)
        self.assertEqual(msg.tpl_data, 3)

    def test_pick_precip_falls_back_to_first_message(self):
        a = grib_message(np.ones(4), 2, 2, 0, 0, cat=15, num=5)
        b = grib_message(np.zeros(4), 2, 2, 0, 0, cat=3, num=1)
        self.assertEqual(grib2.pick_precip(grib2.decode(a + b)).cat, 15)
        self.assertEqual(grib2.pick_precip(grib2.decode(b + grib_message(np.ones(4), 2, 2, 0, 0, cat=1, num=8))).cat, 1)

    def test_unsupported_template_is_reported(self):
        m = bytearray(grib_message(np.ones(4), 2, 2, 0, 0))
        i = m.index(bytes([3]), 16 + 21 + 4)  # section 3 number byte, then its template at octets 13-14
        m[i + 8:i + 10] = (40).to_bytes(2, "big")
        with self.assertRaises(ValueError):
            grib2.decode(bytes(m))


class TestJanela(unittest.TestCase):
    def test_incremental_uses_watermark_and_lookback(self):
        p = J.plan_window("incremental", "2000-06-01", "2026-01-20", "", "", 7, 120, False, "2026-01-05", None)
        self.assertEqual((p["start"], p["end"]), ("2026-01-06", "2026-01-19"))
        p = J.plan_window("incremental", "2000-06-01", "2026-01-20", "", "", 7, 120, False, "2026-01-18", None)
        self.assertEqual(p["start"], "2026-01-13")

    def test_range_and_backfill_cursor(self):
        p = J.plan_window("range", "2000-06-01", "", "1999-01-01", "2000-06-03", 7, 120, False, None, None)
        self.assertEqual(p["candidates"], ["2000-06-01", "2000-06-02", "2000-06-03"])
        p = J.plan_window("backfill", "2026-01-01", "2026-01-11", "", "", 7, 4, False, None, None)
        self.assertEqual((p["start"], p["end"], p["has_more_window"]), ("2026-01-01", "2026-01-04", True))
        st = {"cursor_data": "2026-01-08", "janela_inicio": "2026-01-01", "janela_fim": "2026-01-10"}
        p = J.plan_window("backfill", "2026-01-01", "2026-01-11", "", "", 7, 4, False, None, st)
        self.assertEqual((p["start"], p["end"], p["has_more_window"]), ("2026-01-09", "2026-01-10", False))
        self.assertEqual(J.plan_window("backfill", "2026-01-01", "2026-01-11", "", "", 7, 4, True, None, st)["start"], "2026-01-01")
        with self.assertRaises(ValueError):
            J.plan_window("range", "2000-06-01", "", "2026-01-01", "", 7, 1, False, None, None)

    def test_js_round(self):
        np.testing.assert_array_equal(J.js_round([0.125, 0.135, -0.125, 2.5], 2), [0.13, 0.14, -0.12, 2.5])


class TestIbge(Base):
    def test_snapshot_and_aliases(self):
        r = ibge.main(self.spark, base_url=self.ibge_url, target_db=DB)
        self.assertEqual((r["status"], r["estados_carregados"], r["municipios_carregados"], r["aliases_na_tabela"]), ("ok", 27, 5105, 3))
        m = C.read_table_pd(DB, ibge.TB_MUN).set_index("cod_ibge")
        bh = m.loc["3106200"]
        self.assertEqual((bh["cod_ibge_5"], bh["uf"], bh["municipio_uf"], bh["municipio_norm"], int(bh["capital"])),
                         ("06200", "MG", "Belo Horizonte - MG", "BELO HORIZONTE", 1))
        a = C.read_table_pd(DB, ibge.TB_ALI).set_index("alias_norm")
        self.assertEqual(a.loc["PORTO DE GALINHAS", "cod_ibge"], "2607208")
        ibge.main(self.spark, base_url=self.ibge_url, target_db=DB)  # reload: snapshot + upsert stay idempotent
        self.assertEqual(len(C.read_table_pd(DB, ibge.TB_MUN)), 5105)
        self.assertEqual(len(C.read_table_pd(DB, ibge.TB_ALI)), 3)

    def test_unresolved_alias_fails_before_writing(self):
        seeds = list(ibge.ALIAS_SEEDS)
        ibge.ALIAS_SEEDS.append({"alias": "Nowhere", "uf": "MG", "municipio": "Cidade Inexistente"})
        try:
            with self.assertRaises(ValueError):
                ibge.main(self.spark, base_url=self.ibge_url, target_db="t_never")
        finally:
            ibge.ALIAS_SEEDS[:] = seeds
        self.assertFalse(C.table_exists("t_never", ibge.TB_MUN))


class TestMerge(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ibge.main(cls.spark, base_url=cls.ibge_url, target_db=DB)
        # 0.1° grid over the fixture municipalities, rain = 1 mm everywhere except one pixel; negatives = ocean/missing
        lat0, lon0, nlat, nlon = -21.0, -46.0, 30, 32
        cls.grid = (lat0, lon0, nlat, nlon)
        days = {"2026-01-14": 1.0, "2026-01-15": 2.0, "2026-01-16": 3.0}
        os.makedirs(os.path.join(FIX, "merge"), exist_ok=True)
        for d, base in days.items():
            v = np.full(nlat * nlon, base)
            i = int(np.floor((-19.9102 - lat0) / 0.1 + 0.5))
            j = int(np.floor((-43.9266 - lon0) / 0.1 + 0.5))
            v[i * nlon + j] = 10 * base  # Belo Horizonte pixel
            v[0] = -1  # sanitized to NaN
            with open(os.path.join(FIX, "merge", "MERGE_%s.grib2" % d.replace("-", "")), "wb") as f:
                f.write(grib_message(v, nlat, nlon, lat0, lon0, tpl=3, D=2, cat=15, num=5))
        cls.tpl = "file://" + FIX + "/merge/MERGE_{Y}{m}{d}.grib2"
        orig = merge.fetch_day

        def fetch(url):  # file:// has no 404: a missing fixture plays "not published"
            return orig(url) if os.path.exists(url[len("file://"):]) else None

        merge.fetch_day = fetch

    def run_merge(self, **kw):
        return merge.main(self.spark, url_template=self.tpl, target_db=DB, **kw)

    def test_01_range_load_samples_pixel_and_block(self):
        r = self.run_merge(mode="range", start_date="2026-01-14", end_date="2026-01-17")
        self.assertEqual((r["dias_carregados"], r["dias_nao_publicados"], r["linhas_carregadas"]), (3, ["2026-01-17"], 3 * 5105))
        t = C.read_table_pd(DB, merge.TABLE)
        bh = t[(t.cod_ibge == "3106200")].sort_values("data")
        self.assertEqual([float(x) for x in bh["prec_mm"]], [10.0, 20.0, 30.0])
        self.assertEqual([float(x) for x in bh["prec_mm_3x3"]], [2.0, 4.0, 6.0])  # (10*base + 8*base) / 9
        self.assertEqual(float(bh["pixel_lat"].iloc[0]), -19.9)
        far = t[t.cod_ibge == "2607208"]  # Ipojuca is outside the fixture grid
        self.assertTrue(far["prec_mm"].isna().all() and far["pixel_lat"].isna().all())
        self.assertEqual(C.partitions(DB, merge.TABLE, "ano"), ["2026"])

    def test_02_reload_skips_complete_days_and_force_reloads(self):
        self.run_merge(mode="range", start_date="2026-01-14", end_date="2026-01-16")
        r = self.run_merge(mode="range", start_date="2026-01-14", end_date="2026-01-16")
        self.assertEqual(r["dias_planejados"], 0)
        r = self.run_merge(mode="range", start_date="2026-01-14", end_date="2026-01-16", force=True)
        self.assertEqual(r["dias_carregados"], 3)
        self.assertEqual(len(C.read_table_pd(DB, merge.TABLE)), 3 * 5105)  # upsert, no duplicates
        self.assertEqual(J.watermark(DB, merge.TABLE), "2026-01-16")

    def test_03_backfill_walks_with_cursor(self):
        db = DB + "_bf"
        ibge.main(self.spark, base_url=self.ibge_url, target_db=db)
        kw = dict(mode="backfill", start_date="2026-01-14", end_date="2026-01-17", max_days=2, target_db=db)
        r1 = merge.main(self.spark, url_template=self.tpl, **kw)
        self.assertEqual((r1["janela"], r1["has_more"], r1["cursor"]), ("2026-01-14..2026-01-15", True, "2026-01-15"))
        r2 = merge.main(self.spark, url_template=self.tpl, **kw)
        self.assertEqual((r2["janela"], r2["has_more"], r2["dias_nao_publicados"]), ("2026-01-16..2026-01-17", False, ["2026-01-17"]))
        self.assertEqual(J.ctl(db).read_state("merge", "backfill")["cursor_data"], "2026-01-17")

    def test_04_dry_run_only_plans(self):
        r = merge.main(None, mode="range", start_date="2026-02-01", end_date="2026-02-02", target_db="t_nothing", dry_run=True)
        self.assertEqual(r["a_processar"], ["2026-02-01", "2026-02-02"])
        self.assertFalse(C.table_exists("t_nothing", merge.TABLE))


class TestPower(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ibge.main(cls.spark, base_url=cls.ibge_url, target_db=DB + "_p")
        cls.calls = []

        def fake(t, params, start, end, api=None):
            cls.calls.append((t["latMin"], t["lonMin"], tuple(params), start, end))
            feats = []
            for lat in np.arange(t["latMin"], t["latMax"] + 0.01, 0.5):
                for lon in np.arange(t["lonMin"], t["lonMax"] + 0.01, 0.625):
                    par = {}
                    for p in params:
                        ser = {d.replace("-", ""): (round(-lat + (0.1 if p == "T2M" else 0), 3) if d != end else -999.0)
                               for d in C.days_between(start, end)}
                        par[p] = ser
                    feats.append({"type": "Feature", "geometry": {"coordinates": [float(lon), float(lat), 0]},
                                  "properties": {"parameter": par}})
            return {"type": "FeatureCollection", "header": {"fill_value": -999.0}, "features": feats}

        power.power_request = fake

    def test_load_partial_update_and_fill_values(self):
        db = DB + "_p"
        r = power.main(self.spark, mode="range", start_date="2026-01-01", end_date="2026-01-03", target_db=db)
        self.assertEqual((r["dias_com_dado"], r["ultimo_dia_com_dado"]), (2, "2026-01-02"))  # the last day is a fill value
        self.assertGreater(r["requisicoes"], 0)
        t = C.read_table_pd(db, power.TABLE)
        bh = t[t.cod_ibge == "3106200"].sort_values("data")
        self.assertEqual(len(bh), 2)
        self.assertEqual(float(bh["pixel_lat"].iloc[0]), -20.0)  # nearest node of the 0.5° grid
        self.assertEqual(float(bh["prec_mm"].iloc[0]), 20.0)
        power.main(self.spark, mode="range", start_date="2026-01-01", end_date="2026-01-03", parametros=["T2M"], target_db=db)
        t = C.read_table_pd(db, power.TABLE)
        bh = t[t.cod_ibge == "3106200"].sort_values("data")
        self.assertEqual(len(bh), 2)
        self.assertEqual([float(x) for x in bh["prec_mm"]], [20.0, 20.0])  # kept (partial update)
        self.assertEqual([float(x) for x in bh["t2m_c"]], [20.1, 20.1])

    def test_invalid_parameters(self):
        with self.assertRaises(ValueError):
            power.main(None, parametros=["XYZ"], dry_run=True)
        with self.assertRaises(ValueError):
            power.main(None, tile_deg=1, dry_run=True)

    def test_tiles_cover_points(self):
        pts = J.fetch_points(DB + "_p")
        tiles = power.build_tiles(pts, 10)
        for lat, lon in zip(pts["lat"], pts["lon"]):
            self.assertTrue(any(t["latMin"] - 1 <= lat <= t["latMax"] + 1 and t["lonMin"] - 1 <= lon <= t["lonMax"] + 1 for t in tiles))
        self.assertTrue(all(t["latMax"] - t["latMin"] >= 2 and t["lonMax"] - t["lonMin"] >= 2 for t in tiles))


class TestFlow(unittest.TestCase):
    def test_summary(self):
        self.assertEqual(flow._resumo([{"fonte": "merge", "has_more": True}, {"fonte": "merge", "linhas_carregadas": 9, "has_more": False}])["linhas"], 9)
        self.assertIn("erro", flow._resumo({"error": "boom"}))
        self.assertEqual(flow._resumo({"fonte": "power", "dry_run": True})["status"], "dry_run")


if __name__ == "__main__":
    unittest.main()

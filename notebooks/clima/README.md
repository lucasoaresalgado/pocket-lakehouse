# Climate — daily rain and weather per municipality

PySpark/numpy pipelines that sample two open climate sources at the centroid of each of the 5,571 IBGE municipalities
and store them as Parquet in the lake (bucket `lake`, folder `target_db`, default `estudo`).

New to the platform? Start with the [root README](../../README.md).

## 1. Layout

```
notebooks/clima/
├── 00_flow_clima.ipynb        the flow (MERGE + POWER branches + summary)
├── 01_ibge_municipios.ipynb   reference table: municipality centroids
├── 02_cptec_merge.ipynb       INPE/CPTEC MERGE
├── 03_nasa_power.ipynb        NASA POWER
├── run.py                     command line
├── pipelines/                 ibge.py, merge.py, grib2.py, power.py, janela.py (shared planning), flow.py
└── tests/                     offline tests (synthetic GRIB2, fixtures, mocked API)
```

Shared infrastructure (storage, `_ctl`, HTTP, size guard) is in [`notebooks/lakehouse/core.py`](../lakehouse/core.py).

## 2. Sources and tables

| Source | Origin | Table | Key | Layout |
|---|---|---|---|---|
| **IBGE** municipalities/states/aliases | `github.com/kelvins/municipios-brasileiros` CSVs | `dim_municipiosIbge`, `dim_estadosIbge` (snapshot), `dim_municipiosIbgeAliases` (upsert) | `cod_ibge` / `cod_uf` / `alias_norm`+`uf` | plain |
| **MERGE** CPTEC/INPE daily rain, 0.1° grid, since 2000-06 | one GRIB2 per day on `ftp.cptec.inpe.br` (HTTPS) | `cptec_merge_diario`: `prec_mm` (nearest pixel), `prec_mm_3x3` (3x3 mean), `pixel_lat/lon`, `arquivo` | `data`+`cod_ibge` | partitioned by `ano` |
| **NASA POWER** daily, 0.5°x0.625° grid, since 1981 | REGIONAL API, blocks up to 10°x10° | `nasa_power_diario`: `prec_mm`, `t2m_c`, `t2m_max_c`, `t2m_min_c`, `rh2m_pct`, `ws2m_ms`, `pixel_lat/lon` | `data`+`cod_ibge` | partitioned by `ano` |

MERGE/POWER look for `dim_municipiosIbge` in `ref_db`, then `target_db` (default `estudo`); the flow runs `ibge` first
when it is in neither.

## 3. Run modes (MERGE and POWER)

| Mode | Window |
|---|---|
| `incremental` | `[min(watermark+1, end-lookback+1), end]`, `end = end_date or reference_date-1`. MERGE skips days already complete (≥ 5,000 municipalities); POWER reloads the whole 75-day lookback because NASA revises the last ~2 months |
| `range` | exactly `[start_date, end_date]` (`force=True` re-downloads MERGE days already loaded) |
| `backfill` | walks `[start_date or first day, end]` with a cursor in `_ctl/ctl_clima_extracao.json`, `max_days` per run; returns `has_more` and the flow repeats until it is `False` (`reset_cursor=True` restarts) |
| `dry_run` | plan only (window, days to fetch, request count, size estimate) |

POWER `parametros`: `PRECTOTCORR` (default), `T2M`, `T2M_MAX`, `T2M_MIN`, `RH2M`, `WS2M`. Loading some parameters keeps
the values already stored for the others (partial update).

## 4. Commands

```bash
docker exec spark-jupyter bash -c "cd /notebooks/clima && python3 run.py <ibge|merge|power|flow> [key=value …] [--dry-run] [--cores N] [--local]"

python3 run.py ibge target_db=estudo
python3 run.py merge mode=range start_date=2026-01-01 end_date=2026-01-31 target_db=estudo
python3 run.py power mode=range start_date=2026-01-01 end_date=2026-03-31 'parametros=["PRECTOTCORR","T2M"]' target_db=estudo
python3 run.py flow mode=incremental target_db=estudo --dry-run
```

Rough timings: IBGE ~15 s; MERGE ~1–2 s per day (download + decode); POWER ~1 min per block (17 requests with a 1.5 s pause).

## 5. Size (bucket quota)

Measured Parquet sizes: MERGE ≈ 7 B/row (≈ 40 KB/day), POWER ≈ 5 B/row per parameter. Full histories: MERGE since
2000 ≈ 0.4 GB, POWER since 1981 ≈ 0.45 GB per parameter. Every run estimates its write and refuses one that would not fit
in the free quota (`ignorar_cota=True` skips the estimate; MinIO still enforces the hard limit and a refused write leaves
no stage behind).

## 6. Tests

```bash
docker exec spark-jupyter bash -c "cd /notebooks/clima && python3 -m unittest tests.test_pipelines"   # 17 tests, offline
```

They cover GRIB2 simple packing + bitmap and complex packing with 2nd-order spatial differencing (round trip through a
small encoder), window planning and backfill cursor, IBGE snapshot/aliases, MERGE sampling/upsert/404 days/backfill,
POWER fill values and partial update, and the flow summary.

## 7. Design notes and findings

- Tables are partitioned by year, so an upsert rewrites only the years it touches; run state is in `_ctl/ctl_clima_extracao.json`.
- The GRIB2 decoder is plain numpy (no eccodes). It was cross-checked against an independent decoder on a real MERGE file:
  bit-identical on both messages, including missing values.
- A 403/429 from a server stops the run (same rule as every other pipeline here).

Findings: the MERGE file has two messages — precipitation is category 15/5, not category 1, so the
decoder falls back to the first message; the second one (category 3/1, values 1–33 on ~3,800 pixels) looks like a rain-gauge count and is not used. The 3x3 mean and rounding
follow JavaScript's `Math.round` (half up), not numpy's half-to-even.

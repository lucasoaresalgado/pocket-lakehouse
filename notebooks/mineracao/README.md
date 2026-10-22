# Mining & oil — public data pipelines

The first category of the platform: PySpark pipelines that download open data from Brazil's mining agency (ANM) and oil
agency (ANP), filter what matters, handle personal data (LGPD) and write Parquet tables to the lake. A pandas + DuckDB
notebook then analyzes them.

> **Safety rule:** nothing here connects to a production database. The tables go to MinIO only.

New to the platform? Start with the [root README](../../README.md) (infrastructure, how to start the stack, conventions).

## Contents

1. [Layout](#1-layout)
2. [Sources and tables](#2-sources-and-tables)
3. [The "focus" (what gets extracted)](#3-the-focus-what-gets-extracted)
4. [LGPD](#4-lgpd)
5. [Run modes](#5-run-modes)
6. [The flow](#6-the-flow)
7. [How a run works, step by step](#7-how-a-run-works-step-by-step)
8. [Generated tables](#8-generated-tables)
9. [Run bookkeeping (`_ctl`)](#9-run-bookkeeping-_ctl)
10. [Code: what each module does](#10-code-what-each-module-does)
11. [Notebooks and command line](#11-notebooks-and-command-line)
12. [Tests](#12-tests)
13. [Findings](#13-findings)

---

## 1. Layout

```
notebooks/mineracao/
├── 00_flow_mineracao_petroleo.ipynb     the full flow (6 branches + summary)
├── 01_anm_cfem.ipynb … 06_anm_dipem.ipynb   one notebook per source
├── 10_Analytics.ipynb                   analysis: pandas + DuckDB (SQL) + matplotlib
├── run.py                               runs one pipeline from the command line
├── pipelines/                           the code (section 10)
└── tests/                               tests (section 12)
```

The flow is the extract → transform → analyze path in one place: `pipelines/` extracts and transforms, the lake stores,
`10_Analytics.ipynb` analyzes.

---

## 2. Sources and tables

| Source | Origin | Output table(s) | Strategy | Primary key |
|---|---|---|---|---|
| **CFEM** — mining royalties collected | single ANM CSV (~325 MB, 2.4 M rows) | `anm_cfem_arrecadacao` | month window, **upsert** | `ano_mes` + `chave` |
| **AMB** — raw and processed production | 2 annual ANM CSVs | `anm_amb_producao` | per year, **upsert**, long format | `ano` + `chave` |
| **ANP** — oil/gas production by well | monthly ZIPs discovered on the ANP page | `anp_producao_poco` and `anp_producao_instalacao` | month window with **cursor**, upsert | `ano_mes` + `poco` |
| **SCM** — Mining Registry | ZIP of ~175 MB (~1 GB uncompressed) | `anm_scm_processo`, `_substancia`, `_municipio`, `_pessoa`, `_titulo`, `_evento` | **snapshot** (replaces everything) | per table |
| **SIGBM** — mining dams | single ANM CSV | `anm_sigbm_barragem` | **snapshot** | `id_barragem` |
| **DIPEM** — mineral research investment | single ANM CSV | `anm_dipem_investimento` | **snapshot**, long format | `ano` + `chave` |

*Upsert* = rows with the same key are replaced, the rest stay. *Snapshot* = the whole table is replaced by the current
load (the source delivers the full state).

---

## 3. The "focus" (what gets extracted)

The pipelines do not load everything, only what matters to the portfolio.

- **Substances** (priority): P1 iron · P2 bauxite, phosphate, nickel · P3 copper, manganese, gold, niobium. The
  classification starts from the name published by ANM, normalized (uppercase, no accents), and applies regular-expression
  rules (e.g. "minério de ferro", "hematita", "itabirito" → FERRO).
- **States (UFs):** MG, PA, ES, RJ, MA, GO, AL, SE, BA, SP.
- **Basins (ANP):** Campos, Santos, Sergipe, Alagoas, Espírito Santo.
- These patterns can be overridden by local tables `dim_substanciaMineral` and `dim_alvoExtracao` (Parquet under
  `<cfg_db>/…`); without them the defaults in the code apply.

---

## 4. LGPD

Individuals (PF) are **never** stored with identification:
- **CFEM:** a PF holder becomes the row `'PF'` (no CPF, no process) and is summed per month × municipality × substance.
- **SCM:** PF links become one `'PF'` row per relation type (`n_pessoas`); the **DOU text** (which names the person) is
  discarded for processes where a PF is *Titular\Requerente*, *Arrendatário* or *Cessionário*.
- **SIGBM:** a PF operator (CPF masked at the source) is kept as `'PF'`; technical-manager columns (`RT…`) are not stored.
- **AMB:** identification columns (CPF, name, holder…) are dropped.

---

## 5. Run modes

| Mode | What it does |
|---|---|
| `incremental` | reloads only a recent window (CFEM: 6 months; ANP: 3 months) from the table's *watermark*; **skips the download** if the source did not change (ETag/Last-Modified) |
| `range` | exactly `[start_date, end_date]` |
| `backfill` | history (default since 2015-01). ANP walks in batches of 12 months with a cursor (`has_more`) |
| `profile` | reads, transforms and returns statistics, **without writing** |
| `dry_run` | only builds the plan and checks the source (HEAD + header via Range); does not download the files |

Common parameters: `reference_date`, `start_date`, `end_date` (YYYY-MM or YYYY-MM-DD), `force` (ignores ETag), `target_db`
(prefix in the bucket), `cfg_db`.

> **`target_db` is not a database.** It is just a **prefix (folder)** inside the bucket, playing the role a schema
> (`bronze`, `silver`…) plays in a warehouse. The real data lives in `s3a://lake/estudo/`.

---

## 6. The flow

`run_flow` (`flow.py`) runs the whole flow: six independent branches — CFEM, AMB, ANP (in a loop until
`has_more=False`), SCM, SIGBM, DIPEM — where **one branch failing does not stop the others** (`skip_failure`), plus a
**Summary** step that returns one row per branch. By default it runs sequentially (`parallel=True` uses threads). SCM,
SIGBM and DIPEM only know `incremental`/`backfill`; `range` runs as `incremental`.

This is a plain function call, not a scheduler: nothing triggers it. You run it from the notebook or `run.py`.

---

## 7. How a run works, step by step

Example: `cfem.main(spark, mode="backfill", target_db="estudo")`.

1. **Plan.** Resolves the time window, loads the focus, reads the *watermark* (highest `ano_mes` already stored) and builds
   the plan. With `dry_run` it stops here, after checking the source.
2. **Change detection.** Sends a `HEAD` to the URL and compares ETag/Last-Modified with what is saved in `_ctl`. In
   `incremental`, if equal, it ends with `status: sem_mudanca` (a few seconds).
3. **Download.** Downloads to `data/raw/<source>/` (streaming, decompressing gzip if the server sends it), with a polite
   pause between requests and a `User-Agent` with contact. **A 403/429 ends the run** without insisting.
4. **Read.** The CSV header is read on the driver: it detects the delimiter and encoding (UTF-8 or windows-1252) and
   **maps columns by regular expression** over the normalized name, which tolerates layout changes. If an essential
   column is missing, the run fails with a message explaining it (and the error is recorded in `_ctl`). Spark then reads
   the file in a distributed way.
5. **Transform (Spark, native functions only, no Python UDF).** Normalizes text, converts Brazilian-format numbers
   (`1.234,56`), parses periods, classifies the substance, filters by the focus, resolves the municipality (IBGE code from
   the source, or by UF + name), handles PF and aggregates. Keys (`chave`) are a truncated SHA-256 of the natural fields.
6. **Two-phase write** (`stage` → `commit`). New rows first go to `<table>.__stage`. Only after everything is written is
   the live table swapped: in *upsert*, Spark joins (`left_anti` on the key + new rows) into `.__merge` and the folder is
   replaced; in *snapshot*, the stage becomes the table. If anything fails earlier, the previous table stays intact. In
   SCM, all six tables are staged before the first one is swapped.
7. **Bookkeeping and cleanup.** Records the result in `_ctl` (new ETag, cursor, status) and deletes the temporary
   downloads (`KEEP_RAW=1` keeps them).

---

## 8. Generated tables

Main columns (all also have `loaded_at`):

| Table | Main business columns |
|---|---|
| `anm_cfem_arrecadacao` | `ano_mes`, `uf`, `cod_ibge`, `municipio`, `substancia`, `prioridade`, `tipo_titular` (PJ/PF), `titular_doc` (CNPJ or `PF`), `processo`, `quantidade`, `valor_recolhido` (R$), `n_registros` |
| `anm_amb_producao` | `ano`, `tipo` (BRUTA/BENEFICIADA), `uf`, `substancia`, `metrica` (source column name), `unidade`, `valor`, `atributos` (JSON with text columns) |
| `anp_producao_poco` | `ano_mes`, `poco`, `uf`, `bacia`, `campo`, `operador`, `instalacao`, `oleo`, `condensado`, `petroleo` (bbl/day), `gas_*` (Mm³/day), `agua`, `tempo_producao` |
| `anp_producao_instalacao` | monthly sum of wells per FPSO/platform (materialized on every load) |
| `anm_scm_processo` | `processo`, `processo_chave`, `fase`, `ativo`, `area_ha`, `dt_protocolo`, `substancias_foco`, `ufs` |
| `anm_scm_substancia` / `_municipio` / `_pessoa` / `_titulo` / `_evento` | child tables linked by `processo` (all substances, municipalities, holders, titles and events of the focus processes) |
| `anm_sigbm_barragem` | `id_barragem`, `uf`, `cod_ibge`, `latitude`/`longitude` (decimal degrees), `categoria_risco`, `dano_potencial`, `altura_atual_m`, `volume_atual_m3`, `atributos` |
| `anm_dipem_investimento` | `ano`, `uf`, `cod_ibge`, `substancia`, `tipo_investimento`, `valor_rs` |

**How the sources join:** `anm_scm_processo.processo_chave` (e.g. `812827/1976`) matches `anm_cfem_arrecadacao.processo`
(CFEM only has a process for PJ holders); `cod_ibge` (7 digits) links CFEM, SIGBM, DIPEM and SCM municipalities. CFEM, AMB
and DIPEM also share `ano`/`uf`/`substancia`, which is what `10_Analytics.ipynb` uses for its cross-table queries.

---

## 9. Run bookkeeping (`_ctl`)

`ctl` = *control*. Under `s3a://lake/<target_db>/_ctl/` there are two JSON files that act as control
tables (`ctl_publico_extracao` and `ctl_publico_arquivo`). **They are not analysis data**; only the pipelines use them.
This is idempotency bookkeeping, not run monitoring.

- **`ctl_publico_extracao.json`** — key `source|mode`. Stores the last status (`ok`, `sem_mudanca`, `erro`), the
  `cursor_periodo` (how far it got, to resume the ANP backfill), the window, a result summary and the timestamp.
- **`ctl_publico_arquivo.json`** — key `source|url`. Stores the "fingerprint" of the file already loaded: ETag,
  Last-Modified, size, rows read and written.

They are used to skip downloads when nothing changed, compute the incremental window, resume backfills and record
failures. Deleting one makes the next run treat the source as never loaded (downloads everything again); the data is not
affected. ANP sends neither ETag nor Last-Modified, so its incremental always reloads.

---

## 10. Code: what each module does

Everything in `notebooks/mineracao/pipelines/` (Python 3.8, the one in the Spark image).

| Module | Responsibility |
|---|---|
| `common.py` | shared blocks: Spark session (`get_spark`); text/number/period handling in driver and Spark versions (`norm`, `norm_col`, `parse_num_col`, `parse_periodo_col`); CSV reading and header mapping (`map_header`, `csv_source`); HTTP (`http_open`, `head_info`, `download`, `check_source`); storage-agnostic access via `fsspec` (`table_path`, `uri`, `stage`/`commit`/`load`); the `Ctl` class; the focus (`load_foco`, `classifica_col`); IBGE municipalities (`load_municipios`, `resolve_municipio`) |
| `cfem.py` · `amb.py` · `anp.py` · `scm.py` · `sigbm.py` · `dipem.py` | one per source; each exposes `main(spark, mode, …, dry_run, profile, target_db, cfg_db)` and returns a dict with the result |
| `flow.py` | `run_flow`: runs the six branches and produces the summary |

Design decisions:
- **No Python UDF:** all transformation uses native Spark functions (fast, no Python serialization).
- **Storage-agnostic:** `table_path()` returns `fsspec`-style paths and `uri()` converts to `s3a://` or `file://`.
  Local disk exists only for the offline tests (`ALLOW_LOCAL_STORAGE=1`); in normal use `STORAGE` must be `s3`.
- **Dry run without Spark:** the plan, focus, watermark and `_ctl` are read with pandas/`s3fs`, so a dry run does not even
  open a Spark session.
- **Reads through its own `fs()`:** pandas' `s3fs` keeps a listing cache and, after an upsert, points at files that were
  already replaced; so table reads go through an instance without the cache.

---

## 11. Notebooks and command line

**Notebooks:** `00_flow…` runs everything; `01_…`–`06_…` run one source each; `10_Analytics` reads the Parquet tables with
pandas, queries them with DuckDB (SQL, including `JOIN`s between sources) and plots with matplotlib. Every pipeline
notebook starts with **`dry_run = True`**; change it to `False` to download and write.

**Command line** (`run.py`):
```bash
docker exec spark-jupyter bash -c "cd /notebooks/mineracao && python3 run.py <cfem|amb|anp|scm|sigbm|dipem|flow> [key=value …] [--dry-run] [--profile] [--cores N]"

# examples
python3 run.py sigbm target_db=estudo                                   # full load
python3 run.py cfem mode=range start_date=2025-01 end_date=2025-03 target_db=estudo
python3 run.py cfem mode=backfill target_db=estudo --cores 4            # ~2.5 min
python3 run.py flow mode=incremental --dry-run                          # plan + check of the 6 sources
python3 run.py anp mode=range start_date=2023-01 end_date=2023-01 'urls=["https://…/2023/producao-01.zip"]' target_db=estudo
```
Values are read as JSON when possible (`true`, `12`, `["a","b"]`); the rest stays text.

Rough timings on this cluster (4 cores): SIGBM, DIPEM and AMB ~30 s · CFEM backfill ~2.5 min · ANP (1 month) ~40 s · SCM ~3 min.

**Reading the data from a notebook**
```python
import duckdb, pandas as pd, s3fs

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": "http://minio:9000"}, use_listings_cache=False)
df = pd.read_parquet("lake/estudo/anm_cfem_arrecadacao", filesystem=fs)
df["valor_recolhido"] = df["valor_recolhido"].astype("float64")   # decimals arrive as Python Decimal

duckdb.sql("SELECT uf, SUM(valor_recolhido) FROM df GROUP BY uf").df()   # DuckDB sees `df` by name
# Spark: spark.read.parquet("s3a://lake/estudo/anm_cfem_arrecadacao")
```
`decimal` columns reach pandas as `Decimal`; convert with `astype("float64")` before computing. Otherwise DuckDB infers a
narrow `DECIMAL(13,4)` from the first rows and fails on large values.

---

## 12. Tests

*Offline* tests (sample files on `file://`, no network) and an integration test against the real MinIO.

```bash
docker exec spark-jupyter bash -c "cd /notebooks/mineracao && python3 -m unittest tests.test_pipelines"   # 25 tests (local-disk mode, enabled only here)
docker exec spark-jupyter bash -c "cd /notebooks/mineracao && python3 -m unittest tests.test_s3_storage"  # real MinIO (temporary prefix, deleted at the end)
```
Run the two **in separate commands**: the `common` module is imported once per process and fixes `STORAGE` at import time.

They cover: text/number/period equivalence between driver and Spark; header mapping; ANP link discovery; each pipeline
end to end (filter, aggregation, LGPD, idempotent upsert, watermark window, backfill cursor, snapshot that preserves the
table when the focus comes back empty, synthetic SCM ZIP); gzip decompression; and the MinIO integration.

---

## 13. Findings

**Findings from real runs**
- **ANP 2024 onward has no file link.** The page only lists files up to 2023-12; from 2024 on there is just a "Public Data
  Query". For recent windows the ANP branch ends `ok` with **0 rows** (the plan shows a warning).
- **Pre-salt counted twice (fixed).** Each month brings Land, Sea and Presal, and Presal is a *subset* of Sea. Summing by
  `(month, well)` inflated production (5.71 vs 3.22 million bbl/day in 2023-01). The default now **ignores the Presal
  files**; `excluir_presal=False` restores the double-counting behavior, for comparison.
- **ANP answers 403 to `HEAD`** but serves `GET`: `head_info` tries a 1-byte `GET` (a 403 on that one is still fatal).
- **Servers send gzip without being asked** (IBGE): the HTTP layer decompresses.
- **AMB: `unidade` is often null**, because the files carry units in separate columns ("Unidade de Medida – …"), which go
  to `atributos`.
- **SCM:** 2 processes have child rows with no record in `Processo.txt` (a source inconsistency).

**Validation done:** totals recomputed independently with pandas from the public files match MinIO (CFEM R$ 55.24 bn,
DIPEM R$ 4.28 bn and AMB to the cent; ANP after the pre-salt fix).

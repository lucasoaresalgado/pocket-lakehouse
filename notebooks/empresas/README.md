# Companies — registry, financials, debts, sanctions, procurement and labor (public data)

PySpark pipelines for public data about companies: a flow (RF CNPJ → CVM → PGFN) and standalone pipelines for CGU,
MTE, PNCP and Novo CAGED. Tables are Parquet in the lake (bucket `lake`, folder `target_db`, default `estudo`); text
columns keep the source format, plus normalized join keys.

New to the platform? Start with the [root README](../../README.md).

## 1. Layout

```
notebooks/empresas/
├── 00_flow_empresas.ipynb               RF CNPJ → CVM → PGFN
├── 01_rf_cnpj.ipynb … 07_caged.ipynb    one notebook per source
├── run.py                               command line
├── pipelines/                           cnpj, cvm, pgfn, cgu, mte, pncp, caged, flow
└── tests/                               offline tests (fixture zips, generated xlsx/7z, mocked API)
```

Shared infrastructure is in [`notebooks/lakehouse/core.py`](../lakehouse/core.py). MTE needs `openpyxl` and CAGED needs
`py7zr` (both in `Dockerfile.jupyter`).

## 2. Sources and tables

| Source | Origin | Table(s) | Strategy |
|---|---|---|---|
| **RF CNPJ** | monthly release, Casa dos Dados mirror (37 zips, ~7 GB) | `cnpj_empresas`, `_estabelecimentos`, `_socios`, `_simples`, `_cnaes`, `_municipios`, `_naturezas`, `_motivos`, `_paises`, `_qualificacoes` | snapshot per table |
| **CVM** DFP/ITR | `dados.cvm.gov.br`, one zip per year | `cvm_{dfp\|itr}_{statement}` (e.g. `cvm_dfp_dre_con`) | snapshot of the run's years |
| **PGFN** devedores | `dadosabertos.pgfn.gov.br`, quarterly (3 zips, ~1.4 GB) | `pgfn_devedores` (+ `tipo_divida`) | snapshot of the quarter |
| **CGU** sanções | CGU CDN, daily snapshot (latest only) | `cgu_ceis`, `cgu_cnep`, `cgu_cepim`, `cgu_acordos_leniencia`, `cgu_acordos_leniencia_efeitos`, `cgu_ceaf` | upsert by PK; `data_referencia` = last snapshot seen |
| **MTE** Lista Suja | gov.br spreadsheet (xlsx), ~2 editions/year | `mte_cadastro_empregadores` | upsert by `chave_registro` |
| **PNCP** (Lei 14.133) | API `dadosabertos.compras.gov.br` or monthly CSV zips | `comprasgov_pncp_contratacoes`, `_itens`, `_resultados` | upsert by PK |
| **Novo CAGED** | MTE FTP, `CAGEDMOV{AAAAMM}.7z` | `caged_mov` | partitioned by `competencia_arquivo`; a month reload replaces that month |

Join keys: `cnpj_basico` (8 digits) links CNPJ, CGU and MTE; `cpf_cnpj_norm` (14-digit CNPJ / masked CPF) links CGU, MTE
and PNCP `ni_fornecedor`; the masked CPF `***######**` links sócios (`cnpj_socios.cnpj_cpf_socio`) with CEAF
(`cpf_mascarado_rf`), CEIS/CNEP and MTE. PGFN uses the formatted CNPJ (`12.345.678/0001-90`) in `CPF_CNPJ`.

## 3. Size: what fits in the 5 GiB bucket

| Source | Full size | What to do |
|---|---|---|
| RF CNPJ | ~7 GB zipped → ~6.5 GB Parquet: **does not fit** | `ufs=["ES"]` (≈100k establishments per part for ES) and/or `parts=[…]`; the guard refuses an unfiltered load |
| PGFN | ~1.4 GB zipped | fits, but `ufs=[…]` keeps it small |
| CAGED | ~80 MB per month → full history ~6 GB: **does not fit** | load the months you study (`competencia=AAAAMM` or a year) |
| PNCP CSV | ~35 MB zipped per month | a few years fit; load by `csv_months` / `csv_years` |
| CVM, CGU, MTE | tens of MB | fit |

Every pipeline that can grow estimates its write before downloading and refuses one that would not fit in the free quota
(`ignorar_cota=True` skips the estimate). An upsert also checks that the temporary full copy fits. If MinIO's hard quota
still stops a write, the stage is removed and the live table stays as it was.

## 4. LGPD

Some sources publish **open CPFs** of individuals: MTE (employers), CGU CEIS/CNEP (sanctioned individuals), PNCP
(individual suppliers) and the RF `razao_social` of sole proprietors/MEIs (which ends with the owner's CPF). By default
they are stored with the Receita mask (`***######**`, or `***.###.###-**` where the source uses punctuation), which keeps
every cross-check above working. `manter_cpf_aberto=True` (cnpj, cgu, mte, pncp) stores them open.
Keys are unaffected (MTE's `chave_registro` is still computed from the full document). PGFN, CEAF and the RF sócios are
already masked at the source.

## 5. Commands

```bash
docker exec spark-jupyter bash -c "cd /notebooks/empresas && python3 run.py <cnpj|cvm|pgfn|cgu|mte|pncp|caged|flow> [key=value …] [--dry-run] [--cores N] [--local]"

python3 run.py cnpj 'ufs=["ES"]' target_db=estudo                         # all tables, establishments of ES
python3 run.py cnpj 'tables=["Cnaes","Municipios","Naturezas"]' target_db=estudo
python3 run.py cvm 'years=[2024,2025]' target_db=estudo
python3 run.py pgfn 'ufs=["ES","RJ"]' target_db=estudo
python3 run.py cgu target_db=estudo                                        # dry run downloads + validates, no Spark
python3 run.py mte target_db=estudo
python3 run.py pncp date_start=2026-09-01 date_end=2026-09-30 target_db=estudo
python3 run.py pncp mode=csv 'csv_months=["2026-08","2026-09"]' target_db=estudo
python3 run.py caged competencia=202608 target_db=estudo
python3 run.py flow 'cnpj_ufs=["ES"]' target_db=estudo --dry-run
```

Rough timings on this cluster: CNPJ lookups + one part of Estabelecimentos/Empresas/Socios filtered to ES ~100 s;
CGU (5 registries) ~1 min; CVM DFP one year ~1 min; CAGED one month ~2 min; MTE ~10 s.

## 6. Tests

```bash
docker exec spark-jupyter bash -c "cd /notebooks/empresas && python3 -m unittest tests.test_pipelines"   # 18 tests, offline
```

They cover the CNPJ `ufs` filter and semi-join, MEI masking and a failed part keeping the previous snapshot, the size
guard, CVM statement tables and year union, PGFN types/UF filter and failure handling, CGU derived keys, masking, upsert
across snapshots and header-change refusal, PNCP CSV (quoted newlines, 404 months) and API (pages, modalidades, unknown
fields), MTE parsing/masking/stable keys, CAGED partitions, and the flow summary.

## 7. Design notes and findings

- CNPJ, PGFN and CVM stage every file first and swap the table only when all of them made it (a failed file keeps the
  previous snapshot).
- CAGED is partitioned by month: reloading a month replaces only that month.
- PNCP CSV files come from the repository's monthly layout
  `pncp/{AAAA}/{MM}/{compra|compra_item|item_resultado}_{AAAA}_{MM}.csv.zip` (`;`, UTF-8).
- CGU: if a PK repeats within a snapshot, the last row wins and the count goes to `duplicadas_no_snapshot` (none seen in 10/2026).

Findings from the real runs (10/2026): the PNCP API mirror lags ~4 weeks (no results after ~2026-09-25, and contratações/itens
lag more than resultados), so the default 3-day window returns nothing — use an older window or `mode=csv`. The MTE
server answers 403 to `HEAD` but serves `GET`. The CGU CDN publishes snapshots on different days per registry (CEPIM one
day behind). CAGED 2026-08 has 4.4 M rows (77 MB of Parquet).

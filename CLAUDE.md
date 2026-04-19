# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Hagkerfi** is an Icelandic economic data pipeline and tax simulation system. It downloads statistical data from official Icelandic sources (Hagstofa, HMS, Seðlabanki), extracts the statistical parameters needed to generate a synthetic population (~370K individuals), and simulates tax revenues across multiple tax types.

## Data Architecture Principle

Only `data/silver.duckdb` is permanent. It stores the statistical parameters extracted from official sources. `data/landing/` and `data/bronze/` are temporary pipeline artifacts — deleted automatically after `make silver`. They can be regenerated at any time by re-running the download pipeline. No simulation or reporting code reads from landing/ or bronze/; all reads are from silver.duckdb.

## Tech Stack

- **Database:** DuckDB (`data/silver.duckdb`) — columnar, embedded, OLAP-optimised. All pipeline stages read/write via the `duckdb` Python package. `DB_PATH` env var overrides the default path.
- **Gold export:** Parquet (`data/gold/population.parquet`) via `pyarrow`. DuckDB can query Parquet directly without importing.
- **Data wrangling:** pandas + numpy. DuckDB DataFrames are returned via `.df()` on query results.
- **Visualisation:** matplotlib (Agg backend, charts saved to `reports/charts/`).
- **Downloads:** requests (Hagstofa PX-API), openpyxl / xlrd (Excel files).

## Common Commands

All commands run via Make. The virtual environment is managed locally in `.venv/`.

```bash
make deps           # Create venv and install requirements.txt
make pipeline       # Full run: landing → bronze → silver → clean-temp → gold → simulate → validate
make data           # Download and extract statistics into silver.duckdb, then clean temp files
make population     # Generate population and reports: gold → validate → report
make validate       # QA: compare generated distributions against official data in silver.duckdb
make report         # Build HTML report + tax summary charts
make diagnostics    # Write per-age/gender income error tables to reports/
make clean-temp     # Delete data/landing and data/bronze (safe to run any time)

# Individual pipeline stages:
make landing        # Download raw data from Hagstofa APIs and static files
make bronze         # Convert/clean landing data, interpolate missing values
make silver         # Extract statistics into data/silver.duckdb, then auto-deletes landing/bronze
make gold           # Generate synthetic population, export Parquet, build occupation stats
make simulate       # Run tax calculation; results persisted to population_with_taxes table
```

Run individual Python scripts directly using the local venv:
```bash
.venv/bin/python generate_population.py
.venv/bin/python calculate_taxes.py
DB_PATH=data/silver.duckdb .venv/bin/python pipelines/mint_silver.py
DB_PATH=data/silver.duckdb .venv/bin/python qa/validate_population.py
```

Override defaults with env vars: `DB=custom.duckdb make silver`, `TIMEOUT=600 make gold`.

## Architecture

The system follows a **Medallion Architecture**:

```
APIs + Excel Files
    → data/landing/       (raw: JSON, XLSX — TEMPORARY, auto-deleted after silver)
    → data/bronze/        (cleaned CSVs — TEMPORARY, auto-deleted after silver)
    → data/silver.duckdb  (DuckDB: statistical parameters extracted from official sources)
    → data/gold/          (population.parquet + occupation stats table in silver.duckdb)
    → reports/            (HTML report, charts, diagnostic CSVs)
```

### Pipeline Stages (`pipelines/`)

- **`landing_download.py`** — Downloads from Hagstofa PX-API (population, income, employment, sector employment VIN10022, wages by occupation VIN02003) and static Excel files (national budget, municipal accounts, property tax). New API tables use `download_px_table_dynamic()` which fetches metadata first to discover variable names before querying.
- **`mint_bronze.py`** — Excel→CSV, JSON→CSV, interpolates missing age rows, normalises Icelandic column names. Uses `_decode_px_combined()` to parse the combined `{"variables": [...], "data": [...]}` format saved by the dynamic downloader.
- **`mint_silver.py`** — Loads all bronze CSVs into DuckDB tables by filename (auto-discovers files). Uses `CREATE OR REPLACE TABLE name AS SELECT * FROM df`.
- **`mint_gold.py`** — Runs population generation as subprocess, exports population to Parquet, builds `gold_occupation_income_stats` table.
- **`medallion_pipeline.py`** — Orchestrator running all stages sequentially (alternative to Make).

### Core Logic (root)

- **`generate_population.py`** — Generates synthetic individuals. For **employed** persons: assigns NACE sector from VIN10022 employment distribution (weighted by gender, adjusted by age via `_YOUNG_BOOST`/`_OLDER_BOOST`), assigns ISCO occupational class within sector from `_SECTOR_OCCUPATION_WEIGHTS`, assigns annual wages from VIN02003 mean monthly wages ×12 with ±15% noise. For **all** persons: capital gains and other income from TEK01001 age+gender means. Applies capital gains boost to top 0.1% high-income elderly. Falls back gracefully to hardcoded distributions when `sector_employment` or `wages_by_sector_occupation` tables are absent from silver.duckdb.
- **`calculate_taxes.py`** — Calculates four tax types: state income tax (three brackets), municipal income tax (per-municipality average rate), capital gains tax (22%), fixed fees (radio fee + elderly fund). Applies personal tax credit. Persists results to `population_with_taxes` table.
- **`helpers.py`** — `get_db_path()` (honours `DB_PATH` env, defaults to `data/silver.duckdb`), `format_number()` (ISK with dot separators), `ensure_dir()`, `connect_db()` (returns `duckdb.DuckDBPyConnection`).

### Reporting (`reports/`)

- **`population_report.py`** — HTML report comparing official vs generated statistics; reads property tax/value from silver.duckdb tables, loads budget Excel files from `data/` root, visualises income distributions by age/gender.
- **`tax_summary.py`** — Reads `population_with_taxes` table; produces income comparison charts, employment status charts, and tax distribution charts in `reports/charts/`; compares simulated totals to 2026 budget targets.
- **`population_diagnostics.py`** — MAE and mean error by age bin; writes to `reports/population_diagnostics.txt` and `.csv`.

### QA (`qa/`)

- **`validate_population.py`** — Compares generated population against official statistics in silver.duckdb across five distributions: age distribution, gender ratio, sector/occupation distribution, occupation class bounds, and income means by age group. Exits with code 1 on any failure so `make pipeline` stops. Saves report to `qa/validation_report.txt`.
- **`design_reviews/`** — QA design review documents written before each agent's implementation.

### Deprecated (`deprecated/`)

Legacy scripts from before the medallion pipeline was built. All hardcode the wrong database path (`income_data.db`) and reference tables that no longer exist. Do not use.

## Database

DuckDB at `data/silver.duckdb` (excluded from git). Key tables:

**Loaded from official sources (via landing → bronze → silver):**
- `population_distribution` — (age, population) from Hagstofa MAN00101
- `gender_and_age_income_distribution` — (age, gender, Heildartekjur, Atvinnutekjur, Fjármagnstekjur, Aðrar_tekjur, ...) from Hagstofa TEK01001
- `employment_data` — (age, gender, employed) from Hagstofa VIN10001
- `sector_employment` — (sector, age_group, gender, employment_count, employment_share) from Hagstofa VIN10022 — **optional**, has fallback
- `wages_by_sector_occupation` — (sector, occupational_class, gender, mean_monthly_wage_isk) from Hagstofa VIN02003 — **optional**, has fallback
- `wage_income_distribution` — (gender, age_group, age_min, age_max, percentile, value_isk) from Hagstofa TEK01007 — percentiles 10–99 + conditional mean (-1) of employment income by broad age group; used to sample wages from the real empirical distribution
- `total_income_distribution` — (gender, age_group, age_min, age_max, percentile, value_isk) from Hagstofa TEK01006 — same structure for total income; available for validation/capital-gains modeling
- `withholding_payments` — (gender, age_group, age_min, age_max, payment_type, monthly_isk_per_person) from Hagstofa TEK02011 — monthly per-person amounts for: Launagreiðslur, Lífeyrisgreiðslur, Atvinnuleysisgreiðslur, Fæðingarorlofsgreiðslur, Bætur (disability), Fjárhagsaðstoð, Aðrar; used to assign pension income to Retired and disability/unemployment benefits to Disabled
- `fjarlog_2026`, `arsreikningar_sveitarfelaga` — national budget and municipal accounts (raw CSV dump from Excel)
- `property_tax_amount` — property tax table (columns: tax_total, base_a, base_b, base_c)
- `property_value_estimates` — property value by year (columns: Ár, Fasteignamat)

**Generated by simulation:**
- `population` — (id, age, gender, occupation, sector, wages, capital_gains, other_income, total_income, status)
- `population_with_taxes` — (id, age, income_tax, municipal_tax, capital_gains_tax, fixed_fees, total_tax)
- `gold_occupation_income_stats` — per-occupation income quartiles by age/gender

## DuckDB Usage Pattern

All scripts follow this pattern — do NOT use sqlite3:

```python
import duckdb
from helpers import get_db_path

conn = duckdb.connect(get_db_path())

# Read → always use .df() to get a pandas DataFrame
df = conn.execute("SELECT * FROM some_table").df()

# Write DataFrame → use CREATE OR REPLACE TABLE ... AS SELECT * FROM df_var
# DuckDB resolves local Python variable names in SQL
conn.execute("CREATE OR REPLACE TABLE my_table AS SELECT * FROM df")

conn.close()
```

## Data Sources

**Hagstofa PX-APIs:**
- MAN00101 — Population by age (2024)
- TEK01001 — Income by gender/age (2023): wages, capital gains, other income
- VIN10001 — Employment by age/gender (2024)
- VIN10022 — Employment by NACE sector × age group × gender (12-month average) — for occupation assignment
- VIN02003 — Mean wages by NACE industry × ISCO occupational class × gender (2024) — for relative sector/occupation wage structure
- TEK01007 — Employment income distribution by gender × broad age group (2023): percentiles 10–99 + conditional mean — used as empirical wage distribution base; rescaled per 5-year age group to match TEK01001 Atvinnutekjur / emp_rate
- TEK01006 — Total income distribution by gender × broad age group (2023): same percentile structure — available for capital gains distribution modeling
- TEK02011 — Monthly withholding-obligatory payments by gender × age group × payment type (12-month rolling) — amounts in millions ISK, counts in individuals; per-person = amount × 1,000,000 / count; used for pension (Retired) and disability benefits (Disabled)

**PX-API download pattern for new tables:** `download_px_table_dynamic()` in `landing_download.py` GETs metadata first (to discover variable names without guessing), then POSTs the data query. Saves `{"variables": [...], "data": [...]}` so `_decode_px_combined()` in bronze can decode variable labels without a second network request. **Important:** PX-API response key order follows the TABLE's variable order (from metadata), not the query order.

**Static Excel files** (downloaded or manually placed in `data/landing/`):
- `fjarlog_2026.xlsx` — National budget 2026
- `arsreikningar_sveitarfelaga.xlsx` — Municipal accounts
- `utsvar_sveitarfelaga.xls` — Municipal income tax rates (used directly by `calculate_taxes.py`)
- `property_tax_amount.xlsx` — Property tax table

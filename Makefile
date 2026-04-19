# Choose Python: prefer local venv if present
PY ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIP ?= $(PY) -m pip
PIP_QUIET ?= -q
DB ?= data/silver.duckdb
TIMEOUT ?= 300s

.PHONY: help deps landing bronze silver gold simulate pipeline data population
.PHONY: report diagnostics validate clean-temp web

help:
	@echo "make web        - start the interactive simulation web app (http://localhost:5000)"
	@echo "make deps       - create .venv and install requirements"
	@echo "make landing    - download raw data into data/landing  (temporary)"
	@echo "make bronze     - clean/convert landing data into data/bronze  (temporary)"
	@echo "make silver     - extract statistics into $(DB)  (permanent)"
	@echo "make gold       - generate synthetic population into $(DB)"
	@echo "make simulate   - run tax simulation against $(DB)"
	@echo "make validate   - QA: compare generated population distributions to official data"
	@echo "make pipeline   - full run: landing -> bronze -> silver -> gold -> simulate -> validate"
	@echo "make clean-temp - delete data/landing and data/bronze (raw data, safe to remove)"
	@echo "make report     - build HTML report comparing official vs generated population"
	@echo "make data       - landing -> bronze -> silver -> clean-temp"
	@echo "make population - gold -> validate -> report"
	@echo "make diagnostics- write per-age/gender error tables"

deps: .venv/bin/python
	@$(PIP) install $(PIP_QUIET) -r requirements.txt >/dev/null

.venv/bin/python:
	python3 -m venv .venv

landing: deps
	$(PY) pipelines/landing_download.py

bronze: deps landing
	$(PY) pipelines/mint_bronze.py

# Silver extracts and stores only the statistical parameters needed for population
# generation. Landing and bronze are temporary and can be deleted after this step.
silver: deps bronze
	DB_PATH=$(DB) $(PY) pipelines/mint_silver.py
	DB_PATH=$(DB) $(PY) pipelines/mint_constants.py

gold: deps silver
	DB_PATH=$(DB) timeout $(TIMEOUT) $(PY) pipelines/mint_gold.py

simulate: deps silver
	DB_PATH=$(DB) $(PY) pipelines/run_simulation.py

validate: deps
	DB_PATH=$(DB) $(PY) qa/validate_population.py

pipeline: landing bronze silver clean-temp gold simulate validate

report: deps
	DB_PATH=$(DB) $(PY) reports/population_report.py
	DB_PATH=$(DB) $(PY) reports/tax_summary.py

# data extracts statistics into silver then removes the raw temporary files
data: landing bronze silver clean-temp

population: gold validate report

diagnostics: deps
	DB_PATH=$(DB) $(PY) reports/population_diagnostics.py

# Landing and bronze hold raw downloaded files. They are fully reproducible by
# re-running make landing/bronze and are not needed once silver is built.
web: deps
	@echo "Starting Hagkerfi web app at http://localhost:5000"
	DB_PATH=$(DB) $(PY) web/app.py

clean-temp:
	@echo "Removing temporary raw data (data/landing, data/bronze)..."
	@rm -rf data/landing data/bronze
	@echo "Done. Run 'make landing bronze silver' to regenerate."

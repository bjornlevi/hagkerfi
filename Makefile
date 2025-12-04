# Choose Python: prefer local venv if present
PY ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIP ?= $(PY) -m pip
PIP_QUIET ?= -q
DB ?= data/silver.db
TIMEOUT ?= 300s

.PHONY: help deps landing bronze silver gold simulate pipeline data population
.PHONY: report diagnostics

help:
	@echo "make deps       - create .venv and install requirements"
	@echo "make landing    - download raw data into data/landing"
	@echo "make bronze     - clean/convert landing data into data/bronze CSVs"
	@echo "make silver     - load bronze CSVs into sqlite at $(DB)"
	@echo "make gold       - generate population (DB_PATH=$(DB)) and export data/gold/population.csv"
	@echo "make simulate   - run tax simulation against DB_PATH=$(DB)"
	@echo "make pipeline   - run landing -> bronze -> silver -> gold -> simulate"
	@echo "make report     - build HTML report comparing official vs generated population (DB_PATH=$(DB))"
	@echo "make data       - run landing -> bronze -> silver"
	@echo "make population - run gold -> report"
	@echo "make diagnostics- write per-age/gender error tables"

deps: .venv/bin/python
	@$(PIP) install $(PIP_QUIET) -r requirements.txt >/dev/null

.venv/bin/python:
	python3 -m venv .venv

landing: deps
	$(PY) pipelines/landing_download.py

bronze: deps landing
	$(PY) pipelines/mint_bronze.py

silver: deps bronze
	DB_PATH=$(DB) $(PY) pipelines/mint_silver.py

gold: deps silver
	DB_PATH=$(DB) timeout $(TIMEOUT) $(PY) pipelines/mint_gold.py

simulate: deps silver
	DB_PATH=$(DB) $(PY) pipelines/run_simulation.py

pipeline: landing bronze silver gold simulate

report: deps
	DB_PATH=$(DB) $(PY) reports/population_report.py

data: landing bronze silver

population: gold report

diagnostics: deps
	DB_PATH=$(DB) $(PY) reports/population_diagnostics.py

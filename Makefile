.PHONY: install test

PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

install: ## Create venv, install deps, and register dagster_project package
	python3.12 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

test: ## Run Dagster asset tests (no Docker needed)
	PYTHONPATH=. $(PYTHON) -m pytest tests/ -v

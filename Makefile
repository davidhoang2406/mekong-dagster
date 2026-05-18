.PHONY: install dagster-up dagster-down dagster-shell dagster-logs test

PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

install: ## Create venv, install deps, and register dagster_project package
	python3.12 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

dagster-up: ## Start Dagster webserver + daemon (Docker) → http://localhost:3000
	cd ../mekong-infra && docker compose up -d dagster-webserver dagster-daemon

dagster-down: ## Stop Dagster containers
	cd ../mekong-infra && docker compose stop dagster-webserver dagster-daemon

dagster-shell: ## Open a shell in the dagster-webserver container
	docker exec -it dagster-webserver bash

dagster-logs: ## Tail Dagster webserver + daemon logs
	cd ../mekong-infra && docker compose logs -f dagster-webserver dagster-daemon

test: ## Run Dagster asset tests (no Docker needed)
	PYTHONPATH=. $(PYTHON) -m pytest tests/ -v

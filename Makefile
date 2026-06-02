# =============================================================================
# Nimbus Commerce Platform — developer workflow
# =============================================================================

# --- Python interpreter detection -------------------------------------------
# We probe for the right Python in priority order, with NO hard dependency
# on pyenv shims. Override with:  make install PYTHON=/path/to/python
PYTHON ?= $(shell \
  if   command -v python3.12 >/dev/null 2>&1; then echo python3.12; \
  elif command -v python3    >/dev/null 2>&1; then echo python3; \
  elif command -v python     >/dev/null 2>&1; then echo python; \
  else echo "ERROR: no python found on PATH" >&2; exit 1; \
  fi)

# If we're in a pyenv project (.python-version exists), prefer the
# concrete binary over the shim. This avoids the
# "pyenv: python3.12: command not found" trap when the shim can't
# determine the version.
ifeq ($(wildcard .python-version),.python-version)
  PYENV_VERSION := $(shell cat .python-version)
  PYTHON_BIN := $(shell pyenv which $(PYENV_VERSION) 2>/dev/null || echo $(PYTHON))
else
  PYTHON_BIN := $(PYTHON)
endif

VENV          := .venv
VENV_BIN      := $(VENV)/bin
PY            := $(VENV_BIN)/python
PIP           := $(PY) -m pip

# Use a modern pip/setuptools/wheel. We deliberately allow pip>=24.3 so
# the venv creator gets a PEP 660-capable installer.
PIP_BOOTSTRAP := pip==25.0.1 setuptools==75.6.0 wheel==0.45.1

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "; printf "Nimbus — make targets:\n\n"} \
		/^[a-zA-Z0-9_.-]+:.*?## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' \
		$(MAKEFILE_LIST)

# --- Environment -----------------------------------------------------------
.PHONY: venv
venv: $(VENV)/pyvenv.cfg ## Create the virtualenv.

$(VENV)/pyvenv.cfg:
	@echo ">>> using python interpreter: $(PYTHON_BIN)"
	@$(PYTHON_BIN) -V
	@$(PYTHON_BIN) -m venv $(VENV)
	@$(PY) -m pip install --upgrade $(PIP_BOOTSTRAP)

.PHONY: install
install: venv ## Install project + dev deps into the venv (editable).
	@echo ">>> installing project (editable) + dev extras"
	@$(PIP) install --upgrade $(PIP_BOOTSTRAP)
	@$(PIP) install -e ".[dev]"

.PHONY: install-prod
install-prod: venv ## Install only runtime deps (no dev tooling).
	@$(PIP) install -e .

# --- Quality gates ---------------------------------------------------------
.PHONY: env-check
env-check: ## Validate the active .env file.
	$(PY) scripts/check_env.py

.PHONY: lint
lint: ## Run ruff lint + format check.
	$(VENV_BIN)/ruff check src tests
	$(VENV_BIN)/ruff format --check src tests

.PHONY: format
format: ## Auto-format with ruff.
	$(VENV_BIN)/ruff check --fix src tests
	$(VENV_BIN)/ruff format src tests

.PHONY: typecheck
typecheck: ## Run mypy in strict mode against src/.
	$(VENV_BIN)/mypy src

.PHONY: test
test: ## Run the test suite.
	$(VENV_BIN)/pytest

.PHONY: test-cov
test-cov: ## Run the test suite with coverage.
	$(VENV_BIN)/pytest --cov=src --cov-report=term-missing --cov-fail-under=80

.PHONY: pre-commit
pre-commit: ## Install and run pre-commit against all files.
	$(PIP) install pre-commit
	$(VENV_BIN)/pre-commit install
	$(VENV_BIN)/pre-commit run --all-files

# --- Local infra -----------------------------------------------------------
.PHONY: infra-up
infra-up: ## Start the docker-compose stack (postgres, redis, kafka).
	docker compose up -d
	@echo ">>> waiting for healthchecks..."
	@docker compose ps --format json | python -c "import json,sys,time; \
		deadline=time.time()+60; \
		[exit(0) for s in [json.loads(l) for l in sys.stdin] if s.get('Health')=='healthy'] or exit(1)"

.PHONY: infra-down
infra-down: ## Stop the docker-compose stack.
	docker compose down

.PHONY: infra-logs
infra-logs: ## Tail docker-compose logs.
	docker compose logs -f

# --- App -------------------------------------------------------------------
.PHONY: run
run: ## Run the FastAPI app via uvicorn (auto-reload).
	$(VENV_BIN)/uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

.PHONY: run-prod
run-prod: ## Run uvicorn in production mode (no reload).
	$(VENV_BIN)/uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 4

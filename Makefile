.PHONY: setup lint format test

PYTHON ?= python3.12
VENV := .venv

setup:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -r requirements-dev.txt
	$(VENV)/bin/pre-commit install

lint:
	$(VENV)/bin/ruff check .
	$(VENV)/bin/black --check .

format:
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/black .

test:
	$(VENV)/bin/pytest

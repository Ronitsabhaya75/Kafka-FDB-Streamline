.PHONY: setup lint format test test-packaging

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

test-packaging:
	PACKAGING_INTEGRATION=1 $(VENV)/bin/pytest tests/test_packaging.py

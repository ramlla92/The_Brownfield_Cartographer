# Makefile — Brownfield Cartographer

.PHONY: install install-dev test lint format check clean analyze-jaffle analyze-airflow

# ─── Setup ────────────────────────────────────────────────────────────────────

install:
	uv sync

install-dev:
	uv sync --all-extras

# ─── Quality ──────────────────────────────────────────────────────────────────

lint:
	uv run ruff check src/ tests/

format:
	uv run black src/ tests/
	uv run ruff check --fix src/ tests/

check: lint
	uv run mypy src/

# ─── Tests ────────────────────────────────────────────────────────────────────

test:
	uv run pytest tests/ -v --tb=short

test-cov:
	uv run pytest tests/ --cov=src --cov-report=term-missing --cov-report=html

# ─── Analysis Targets ─────────────────────────────────────────────────────────

analyze-jaffle:
	uv run cartographer analyze https://github.com/dbt-labs/jaffle_shop \
		--output .cartography/jaffle_shop \
		--no-llm

analyze-airflow:
	uv run cartographer analyze https://github.com/apache/airflow \
		--output .cartography/airflow \
		--no-llm

analyze-self:
	uv run cartographer analyze . \
		--output .cartography/self \
		--no-llm

analyze-local:
	@echo "Usage: make analyze-local REPO=path/to/repo"
	uv run cartographer analyze $(REPO) --output .cartography/local --no-llm

# ─── Query Shortcuts ──────────────────────────────────────────────────────────

query-blast:
	@echo "Usage: make query-blast REPO=. MODULE=src/agents/surveyor.py"
	uv run cartographer query $(REPO) --tool blast_radius --arg $(MODULE)

# ─── Cleanup ──────────────────────────────────────────────────────────────────

clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache .mypy_cache htmlcov
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

clean-repos:
	rm -rf _repos/

clean-all: clean clean-repos
	rm -rf .cartography/ semantic_index/

.PHONY: setup clean format format-check lint lint-check test

PY_FILES = src tests scripts

# Setup development environment
setup:
	uv sync

# Format code with ruff
format:
	uv run ruff format $(PY_FILES)
format-check:
	uv run ruff format --check $(PY_FILES)

# Lint with ruff and mypy
lint: format
	uv run ruff check --fix $(PY_FILES)
	uv run mypy $(PY_FILES)
lint-check: format-check
	uv run ruff check $(PY_FILES)
	uv run mypy $(PY_FILES)

# Run tests
test:
	uv run pytest

# Clean up build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/ 

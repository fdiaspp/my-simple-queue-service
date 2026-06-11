.PHONY: install run test clean

install:
	uv sync --extra test

run:
	uv run uvicorn app.main:app --reload

test:
	uv run pytest

clean:
	rm -rf .venv .pytest_cache __pycache__ app/__pycache__ tests/__pycache__ .data

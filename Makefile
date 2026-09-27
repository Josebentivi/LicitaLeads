.PHONY: install migrate run test lint format crawl pipeline

install:
	python -m pip install -e ".[dev]"

migrate:
	alembic upgrade head

run:
	python run.py

test:
	pytest

lint:
	ruff check .
	mypy app

format:
	ruff format .

crawl:
	python -m app.cli crawl pncp --uf MA --days 7

pipeline:
	python -m app.cli run-pipeline --uf MA --days 7


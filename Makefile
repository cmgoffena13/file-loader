format: lint
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

install:
	uv sync --frozen --compile-bytecode

test:
	uv run -- pytest -v

dev: reset
	docker compose up -d
	sleep 5
	uv run python main.py

reset:
	cp -R src/tests/archive_data/* src/tests/test_data/
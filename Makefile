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
	@if [ -d "src/tests/archive_data" ] && [ -n "$(ls -A src/tests/archive_data 2>/dev/null)" ]; then \
		cp src/tests/archive_data/* src/tests/test_data/ 2>/dev/null || true; \
	fi
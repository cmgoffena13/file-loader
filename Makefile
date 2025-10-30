format: lint
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

run:
	docker compose up -d
	sleep 5
	uv run python main.py
	
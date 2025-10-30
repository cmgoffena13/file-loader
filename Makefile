format: lint
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

run:
	docker compose up -d
	sleep 5
	uv run python main.py
	
reset:
	@echo "Resetting test data from archive..."
	@if [ -d "src/tests/archive_data" ] && [ -n "$$(ls -A src/tests/archive_data 2>/dev/null)" ]; then \
		cp src/tests/archive_data/* src/tests/test_data/ 2>/dev/null || true; \
		echo "Files restored from archive to test_data"; \
	else \
		echo "No files found in archive_data directory"; \
	fi
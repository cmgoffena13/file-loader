format: lint
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

install:
	uv sync --frozen --compile-bytecode

test:
	uv run -- pytest -v -n auto

dev: dev-postgres

profile: reset
	docker compose up -d postgres
	sleep 1
	DEV_DATABASE_URL=postgresql+psycopg://fileloader:fileloader@localhost:5432/fileloader uv run scalene main.py

down:
	docker compose down

dev-postgres: reset
	docker compose up -d postgres
	sleep 1
	DEV_DATABASE_URL=postgresql+psycopg://fileloader:fileloader@localhost:5432/fileloader uv run python main.py

dev-mysql: reset
	docker compose up -d mysql
	sleep 1
	DEV_DATABASE_URL=mysql+pymysql://fileloader:fileloader@localhost:3306/fileloader uv run python main.py

dev-sqlserver: reset
	docker compose up -d sqlserver sqlserver-init
	sleep 5
	DEV_DATABASE_URL=mssql+pyodbc://sa:FileLoader123!@localhost:1433/fileloader?driver=ODBC+Driver+17+for+SQL+Server uv run python main.py

reset:
	cp -R src/tests/archive_data/* src/tests/test_data/
	rm -rf src/tests/duplicate_files_data/*
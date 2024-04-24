install:
	cd src && \
	pip install -r requirements.txt

venv:
	cd src && \
	python3 --version && python3 -m venv venv

db-migration:
	python3 -m src.db_migration.ddl
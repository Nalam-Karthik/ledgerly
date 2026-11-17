.PHONY: install up down test lint migrate smoke load

API_PORT ?= 8000
export API_PORT

install:
	python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

up:
	docker compose up -d --build

down:
	docker compose down -v

test:
	docker compose up -d postgres redis
	.venv/bin/pytest -q --cov=app --cov=workers

lint:
	.venv/bin/ruff check .

migrate:
	.venv/bin/alembic upgrade head

smoke:
	.venv/bin/python scripts/smoke.py http://localhost:$(API_PORT)

load:
	.venv/bin/locust -f loadtest/locustfile.py --host http://localhost:$(API_PORT) --headless -u 100 -r 20 -t 60s --only-summary

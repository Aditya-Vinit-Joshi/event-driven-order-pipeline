.PHONY: help install up down logs test lint fmt seed dlq scale clean

help:
	@echo "make up        - build & start the whole pipeline (docker compose)"
	@echo "make down      - stop and remove containers"
	@echo "make logs      - tail all service logs"
	@echo "make seed      - POST a sample order to the running API"
	@echo "make test      - run unit tests"
	@echo "make lint      - ruff lint"
	@echo "make dlq       - show dead-letter queue stats"
	@echo "make scale     - run with 3 enrichment + 2 persistence consumers"
	@echo "make load      - run the Locust load test (headless, 60s)"

install:
	pip install -r requirements-dev.txt

up:
	docker compose up --build -d

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f

scale:
	docker compose up --build -d --scale validation=2 --scale enrichment=3 --scale persistence=2

seed:
	curl -s -X POST http://localhost:8000/orders \
	  -H 'Content-Type: application/json' \
	  -d '{"customer_id":"cust_123","currency":"USD","items":[{"product_id":"sku-1","quantity":2,"unit_price":19.99}]}'

test:
	pytest -q

lint:
	ruff check app tests

fmt:
	ruff format app tests

dlq:
	python -m app.tools.dlq stats

load:
	locust -f locust/locustfile.py --host http://localhost:8000 \
	  --headless -u 50 -r 10 -t 60s

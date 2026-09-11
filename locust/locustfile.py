"""Locust load test for the Producer API.

Run against a live stack:

    locust -f locust/locustfile.py --host http://localhost:8000

Or headless for repeatable numbers (the values that fill the resume bullet):

    locust -f locust/locustfile.py --host http://localhost:8000 \
        --headless -u 100 -r 20 -t 2m --csv results/run

Scale consumers between runs to measure horizontal scalability:

    docker compose up -d --scale enrichment=1   # baseline
    docker compose up -d --scale enrichment=3   # scaled out
"""
from __future__ import annotations

import random

from locust import HttpUser, between, task

_PRODUCTS = [f"sku-{i}" for i in range(1, 51)]
_CUSTOMERS = [f"cust_{i}" for i in range(1, 200)]


def _random_order() -> dict:
    n_items = random.randint(1, 4)
    return {
        "customer_id": random.choice(_CUSTOMERS),
        "currency": "USD",
        "items": [
            {
                "product_id": random.choice(_PRODUCTS),
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(1.99, 499.99), 2),
            }
            for _ in range(n_items)
        ],
    }


class OrderUser(HttpUser):
    # Model a fairly aggressive client; tune with -u / -r on the CLI.
    wait_time = between(0.05, 0.3)

    @task(10)
    def create_order(self) -> None:
        with self.client.post(
            "/orders", json=_random_order(), catch_response=True
        ) as resp:
            if resp.status_code != 202:
                resp.failure(f"expected 202, got {resp.status_code}")

    @task(1)
    def health(self) -> None:
        self.client.get("/health")

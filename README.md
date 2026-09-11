# Event-Driven Order Processing Pipeline

A cloud-agnostic, event-driven backend that decouples order **ingestion →
validation → enrichment → persistence** into independently scalable
consumer-group services, with dead-letter handling, exponential-backoff
retries, idempotent writes, and end-to-end distributed tracing.

> Built on vendor-neutral building blocks (Redis Streams, PostgreSQL,
> OpenTelemetry) so the same design maps cleanly onto Azure Service Bus /
> Event Hub, Cosmos DB, AWS SQS/Kinesis, or GCP Pub/Sub.

---

## Architecture

```
 HTTP POST /orders
        │  202 Accepted (async)
        ▼
 ┌─────────────┐   XADD    ┌──────────────────┐
 │ Producer API │ ────────▶ │ orders:created    │
 │  (FastAPI)   │           └───────┬──────────┘
 └─────────────┘                   │ XREADGROUP (validation-workers)
                                    ▼
                          ┌──────────────────┐   XADD   ┌──────────────────┐
                          │ Validation        │ ───────▶ │ orders:validated  │
                          │ Consumer          │          └───────┬──────────┘
                          └──────────────────┘                  │ (enrichment-workers)
                                                                 ▼
                                                        ┌──────────────────┐   XADD
                                                        │ Enrichment        │ ───────▶ orders:enriched
                                                        │ Consumer          │
                                                        └──────────────────┘                  │ (persistence-workers)
                                                                                               ▼
                                                                                     ┌──────────────────┐
                                                                                     │ Persistence       │──▶ PostgreSQL
                                                                                     │ Consumer          │   (idempotent upsert)
                                                                                     └──────────────────┘

 Any stage failing after N retries ──▶ orders:dlq (dead-letter stream)
 All services ──▶ OTel Collector ──▶ Jaeger (one connected trace per order)
```

Each arrow between stages is a Redis Stream; each consumer reads via a
**consumer group** so instances share the load and Redis tracks per-message
ownership in a Pending Entries List (PEL).

---

## Why these choices

| Concern | Choice | Rationale |
|---|---|---|
| Messaging | **Redis Streams** | Consumer groups, per-message ownership, and a PEL for retry/recovery — the same primitives as Kafka/Service Bus, but one container and no broker cluster to operate. |
| Delivery semantics | **At-least-once + idempotent writes** | Redis Streams redeliver un-acked messages; the DB upsert on `order_id` makes duplicate delivery a no-op. |
| Failure handling | **Typed errors → retry vs. DLQ** | Business-rule violations (`PermanentError`) skip retries and dead-letter immediately; transient faults retry with exponential backoff, then dead-letter — nothing is silently dropped. |
| Crash recovery | **`XAUTOCLAIM`** | A worker that dies mid-message leaves it in the PEL; another worker reclaims it after an idle timeout (Redis' equivalent of Kafka offset recovery). |
| Observability | **OpenTelemetry + Jaeger** | `traceparent` is propagated *through the stream messages*, so one trace spans API → publish → 3 consumers → DB. |
| Scaling | **Stateless consumers, one image** | `docker compose up --scale enrichment=3` adds workers to a group with zero code change. |

---

## Quick start

```bash
# 1. Bring up the entire pipeline (Redis, Postgres, Jaeger, OTel Collector,
#    producer API, and all three consumers) with one command:
docker compose up --build

# 2. Submit an order:
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"cust_123","currency":"USD",
       "items":[{"product_id":"sku-1","quantity":2,"unit_price":19.99}]}'
# -> 202 Accepted  {"order_id":"ord_...","status":"created", ...}

# 3. Watch it flow through the stages:
docker compose logs -f validation enrichment persistence

# 4. Confirm it landed in PostgreSQL:
docker compose exec postgres psql -U orders -d orders \
  -c 'SELECT order_id, status, grand_total, warehouse FROM orders;'
```

| Endpoint | URL |
|---|---|
| Producer API docs (Swagger) | http://localhost:8000/docs |
| Health check | http://localhost:8000/health |
| Jaeger tracing UI | http://localhost:16686 |

---

## Reliability features you can demo

### Retries + dead-letter queue (chaos testing)
Inject failures with the `FAIL_RATE` env var to exercise the retry and DLQ
paths on demand:

```bash
FAIL_RATE=0.4 docker compose up -d   # ~40% of consumer steps fail transiently
```

Transient failures retry with exponential backoff (`0.5s → 1s → 2s → 4s`,
jittered). Only if every attempt fails does the message move to the DLQ —
never dropped. Inspect and replay it:

```bash
python -m app.tools.dlq stats     # counts by reason / source stream
python -m app.tools.dlq list      # show dead-lettered messages
python -m app.tools.dlq replay    # re-inject originals upstream to retry
```

### Idempotency
Persistence uses `INSERT ... ON CONFLICT (order_id) DO UPDATE`, so
at-least-once redelivery refreshes the row instead of creating duplicates.

### Horizontal scaling
```bash
docker compose up -d --scale validation=2 --scale enrichment=3 --scale persistence=2
```
Each replica joins its consumer group under a unique name and picks up a share
of the stream automatically.

---

## Distributed tracing

Open http://localhost:16686, pick a service, and view a trace. A single order
produces **one connected trace** spanning:

```
producer: POST /orders → publish order.created
   └─ validation-workers process → publish order.validated
        └─ enrichment-workers process → publish order.enriched
             └─ persistence-workers process → INSERT orders
```

This works because the W3C `traceparent` header is injected into each stream
message and extracted by the next consumer (see `app/telemetry.py`) —
auto-instrumentation alone can't cross a message bus.

---

## Local development

```bash
python -m venv .venv && . .venv/Scripts/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements-dev.txt

pytest -q            # unit + consumer-flow tests (fake Redis, no infra needed)
ruff check app tests # lint
```

Run a single service against local Redis/Postgres (see `.env.example`):

```bash
uvicorn app.producer.main:app --reload      # producer API
python -m app.consumers.validation          # a validation worker
python -m app.consumers.enrichment          # an enrichment worker
python -m app.consumers.persistence         # a persistence worker
```

---

## Project layout

```
app/
├── config.py           # one env-driven Settings object shared everywhere
├── schemas.py          # Pydantic models + the OrderEvent stream envelope
├── errors.py           # PermanentError vs TransientError (drives retry/DLQ)
├── retry.py            # exponential-backoff retry
├── chaos.py            # FAIL_RATE failure injection
├── streams.py          # Redis Stream publish / DLQ / group bootstrap
├── telemetry.py        # OpenTelemetry + traceparent propagation across streams
├── db.py               # asyncpg pool + idempotent upsert
├── consumer_base.py    # reusable consumer-group worker loop (read/retry/DLQ/ack)
├── producer/main.py    # FastAPI producer
├── consumers/          # validation / enrichment / persistence stages
└── tools/dlq.py        # DLQ inspector + reprocessor CLI
db/init.sql             # orders table schema
locust/locustfile.py    # load test
otel-collector-config.yaml
docker-compose.yml      # full stack, one command
.github/workflows/ci.yml# lint + test + build/push image
```

---

## Testing

- **Unit / integration tests** (`pytest`): schema validation, exponential-backoff
  retry semantics, per-stage business logic, and the full consumer loop
  (forward-on-success, DLQ-on-permanent-error, retry-then-DLQ-on-transient) —
  all against an in-memory fake Redis, so `pytest` needs no running infra.
- **CI** (`.github/workflows/ci.yml`): ruff lint → pytest → Docker build, with
  image push to GHCR on `main`.

---

## Load testing

A Locust scenario is included (`locust/locustfile.py`). To reproduce the
scaling numbers:

```bash
docker compose up -d --scale enrichment=1                 # baseline
locust -f locust/locustfile.py --host http://localhost:8000 \
       --headless -u 100 -r 20 -t 2m --csv results/1x

docker compose up -d --scale enrichment=3                 # scaled out
locust -f locust/locustfile.py --host http://localhost:8000 \
       --headless -u 100 -r 20 -t 2m --csv results/3x
```

Measured 2026-07-09 on Windows 11 / Docker Desktop (Hyper-V backend, VM
limited to **2 CPUs / 2 GB RAM** — numbers are conservative for the hardware).
`POST /orders`, 100 users, 20/s ramp, 2 minutes per run:

| Consumers | Requests | Sustained req/s | p50 | p95 | p99 | Error rate |
|---|---|---|---|---|---|---|
| 1× (1/1/1) | 17,995 | 150.3 | 380 ms | 810 ms | 1,200 ms | 0.00% |
| Scaled (2/3/2) | 14,373 | 120.4 | 540 ms | 1,100 ms | 1,300 ms | 0.00% |

**Honest read of the results:** at this load the bottleneck is the producer
API (a single uvicorn worker) sharing 2 vCPUs with the whole stack — a single
consumer per stage already processed the stream in real time (zero lag, zero
pending, all orders persisted before the run ended). Scaling consumers 2–3×
added CPU contention on the same 2-vCPU VM and *reduced* API throughput;
consumer scaling pays off when consumers, not ingestion, are the bottleneck
(e.g. slower per-message work or more CPU headroom). At low concurrency
(10 users) API median latency is ~6 ms — the 380–540 ms medians above are
queueing under 100 concurrent users, not processing cost.

**Chaos / zero-loss verification** (`FAIL_RATE=0.3`, 30% of consumer steps
fail): 1,397 orders sent → 1,364 persisted after automatic retries + 34
dead-lettered after 4 attempts each (DLQ preserved payload, reason, attempts,
traceparent). `python -m app.tools.dlq replay` re-injected all 34 → final DB
count matched every order ever sent. **Zero messages lost.**

---

## Tech stack

Python 3.11 · FastAPI · Redis Streams (`redis-py` async) · PostgreSQL
(`asyncpg`) · OpenTelemetry + Jaeger · Docker Compose · GitHub Actions · Locust

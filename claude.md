# Event-Driven Order Processing Pipeline — Build Spec

A cloud-agnostic, event-driven backend system for resume/portfolio purposes. Built to make the following resume bullets literally true.

---

## ⚠️ BUILD SCOPE — READ FIRST

This spec describes the **full, final version** of the project (see "Target Resume Bullets" and full architecture below). Do NOT build all of it in one sitting, and do NOT put resume bullets referencing DLQ, tracing, or load-test numbers until those specific pieces are actually built and verified.

### Phase 1 — MINIMUM VIABLE SCOPE (build this first, today)
Only these pieces:
1. Producer API (FastAPI) — `POST /orders`, Pydantic validation, publishes `order.created` event to a Redis Stream, returns `202 Accepted`.
2. ONE consumer — reads from the Redis Stream via a consumer group, validates order, writes to PostgreSQL with idempotent upsert (unique constraint on order ID).
3. Retry logic — exponential backoff wrapper around the consumer's processing step.
4. Docker Compose — Redis + Postgres + the two services, one command to run everything.

**Explicitly OUT of scope for Phase 1:** dead-letter stream, second/third consumer (validation vs enrichment split), OpenTelemetry tracing, Locust load testing, GitHub Actions CI/CD.

**Phase 1 resume bullet (only use this one until Phase 2/3 are done):**
> Built an event-driven order processing pipeline with FastAPI and Redis Streams, decoupling order ingestion from validation and persistence via consumer groups, with idempotent writes to PostgreSQL and exponential-backoff retry handling for transient failures.

### Phase 2 — add later
Dead-letter stream, split into Validation + Enrichment + Persistence consumers, OpenTelemetry tracing.

### Phase 3 — add later
Locust load testing (real numbers only), GitHub Actions CI/CD.

**Only add a resume bullet for Phase 2/3 features once they are built and you have run/verified them yourself.** The "Target Resume Bullets" section below is the end-state goal, not what to claim today.

---

## Target Resume Bullets (END STATE — do not use until Phases 2 & 3 are complete)

**Project: Event-Driven Order Processing Pipeline**
*Technologies: Python, FastAPI, Redis Streams, PostgreSQL, Docker, OpenTelemetry, GitHub Actions, Locust*

- Designed and built an event-driven order-processing pipeline using FastAPI and Redis Streams, decoupling ingestion, validation, and persistence into independently scalable consumer-group services.
- Implemented dead-letter stream handling and exponential-backoff retry logic, ensuring zero message loss under simulated consumer failures.
- Instrumented distributed tracing with OpenTelemetry across all services, enabling end-to-end request visibility across the event pipeline.
- Load-tested with Locust, scaling from 1 to 3 consumer instances to sustain [X] events/sec at p95 latency under [Y]ms, demonstrating horizontal scalability improvements.
- Containerized all services with Docker and automated build/test/deploy with GitHub Actions CI/CD.

> Note: [X] and [Y] get filled in with real numbers once load testing is done. Don't estimate — measure.

---

## Why This Project

Targets specific gaps identified against a Microsoft SDE (Commercial Engineering & AI) job description:
- Messaging/event patterns (Service Bus/Event Hub equivalent → Redis Streams, vendor-neutral, reinforces existing Redis skill on resume)
- Cloud storage/database (Cosmos DB equivalent → PostgreSQL, reinforces existing resume skill)
- Telemetry and observability (OpenTelemetry)
- CI/CD and safe deployment practices (GitHub Actions, staged rollout)
- Reliability engineering (DLQ, retries, live-site-style failure handling)

Kept cloud-agnostic (no Azure-specific services) so the resume reads equally well for Microsoft, other big tech, and startups.

**Why Redis Streams over Kafka:** much lower setup overhead (no Zookeeper/broker cluster, no partition/replication config) — one container, running in minutes. Redis Streams still has consumer groups, per-consumer message ownership, and pending-entry lists (used for retry/DLQ logic), so it preserves the same core concepts a message-queue-based system needs, just at a scale appropriate for a solo project. You already list Redis as a skill from your GEP work, so this reinforces real experience instead of introducing a brand-new one from scratch.

---

## Architecture Overview

```
                     ┌─────────────────┐
   HTTP POST         │  Producer API    │
  (order request) ──▶│   (FastAPI)      │
                     └────────┬─────────┘
                              │ XADD "order.created"
                              ▼
                     ┌─────────────────┐
                     │  Redis Stream     │
                     │  (orders:created) │
                     └───┬─────────┬────┘
                         │         │
             ┌───────────┘         └───────────┐
             ▼                                  ▼
   ┌──────────────────┐              ┌──────────────────┐
   │ Validation        │              │ Enrichment        │
   │ Consumer           │──XADD──────▶│ Consumer           │
   │ (consumer group)   │  "order.    │ (consumer group)   │
   │                    │  validated" │                    │
   └──────────────────┘              └────────┬─────────┘
                                                │ XADD "order.enriched"
                                                ▼
                                     ┌──────────────────┐
                                     │ Persistence        │
                                     │ Consumer            │
                                     │ (writes to DB)      │
                                     └────────┬─────────┘
                                                ▼
                                     ┌──────────────────┐
                                     │   PostgreSQL       │
                                     └──────────────────┘

  Failed messages (any stage) ──▶ Dead Letter Stream (orders:dlq)
  All services ──▶ OpenTelemetry Collector ──▶ Jaeger/Grafana (tracing)
```

---

## Components

### 1. Producer API (FastAPI)
- `POST /orders` — accepts an order payload, validates shape, `XADD`s an `order.created` event to the Redis Stream `orders:created`.
- Returns `202 Accepted` with an order ID (async processing, not synchronous).
- Basic input validation with Pydantic models.

### 2. Validation Consumer
- Reads from `orders:created` via a Redis consumer group (`XREADGROUP`).
- Applies business rule checks (e.g., valid quantity, valid product ID, price sanity check).
- On success: `XADD`s to `orders:validated`, then `XACK`s the original message.
- On failure: publishes to the DLQ stream with failure reason attached, then `XACK`s.

### 3. Enrichment Consumer
- Reads from `orders:validated` via its own consumer group.
- Simulates enrichment (e.g., attach mock pricing/tax calculation, mock inventory check).
- `XADD`s to `orders:enriched`, then `XACK`s.

### 4. Persistence Consumer
- Reads from `orders:enriched` via its own consumer group.
- Writes final order record to PostgreSQL.
- Idempotent writes (handle duplicate delivery safely — use order ID as unique constraint).
- `XACK`s after successful write.

### 5. Dead Letter Stream Handling
- Separate Redis Stream: `orders:dlq`.
- Any consumer that fails processing (after N retries with exponential backoff) `XADD`s the original message + error metadata to `orders:dlq` instead of dropping it, then `XACK`s the original so it doesn't block the stream.
- Unacknowledged messages that a consumer crashed on can be reclaimed with `XCLAIM`/`XAUTOCLAIM` from the Pending Entries List (PEL) — this is Redis Streams' equivalent of Kafka's consumer offset recovery.
- Optional: a small DLQ viewer/reprocessor script.

### 6. Retry Logic
- Each consumer wraps processing in a retry decorator: exponential backoff (e.g., 1s, 2s, 4s), max 3-5 attempts before DLQ.
- Simulate failures with a chaos flag/env var (e.g., `FAIL_RATE=0.1`) to test retry and DLQ behavior on demand.

### 7. Observability (OpenTelemetry)
- Instrument all services (producer + 3 consumers) with OpenTelemetry SDK.
- Export traces to Jaeger (run locally via Docker) for end-to-end trace visualization across the pipeline.
- Each trace should show: API request → Redis Stream publish → consumer processing → DB write, as a single connected trace.

### 8. Containerization
- Each service gets its own `Dockerfile`.
- `docker-compose.yml` spins up: Redis, PostgreSQL, Producer API, 3 consumers, Jaeger, OTel Collector.
- One command (`docker compose up`) should bring up the entire system locally.

### 9. CI/CD (GitHub Actions)
- On push: lint (ruff/flake8), run unit tests (pytest), build Docker images.
- On merge to main: build + push images (to GitHub Container Registry — free, no cloud vendor needed).
- Optional: a manual-approval "deploy" stage to simulate staged rollout, even if it just deploys to a free-tier host (Render/Fly.io) or stays as a no-op stage for demonstration purposes.

### 10. Load Testing (Locust)
- Write a Locust file simulating concurrent `POST /orders` traffic.
- Run 3 scenarios: 1 consumer instance, then scale to 2, then 3 (via `docker compose up --scale`, using distinct Redis consumer names within the same group).
- Record: requests/sec sustained, p50/p95/p99 latency, error rate, for each scenario.
- This produces the real [X] events/sec and [Y]ms p95 numbers for the resume bullet.

---

## Build Order (Suggested)

1. Producer API + Redis Stream setup (get one event flowing end to end, log it, no DB yet)
2. Persistence Consumer + PostgreSQL (simplest consumer first — prove the full loop works)
3. Validation Consumer (add business logic + first real DLQ path)
4. Enrichment Consumer (completes the 3-stage pipeline)
5. Retry + DLQ logic (retrofit into all consumers)
6. OpenTelemetry instrumentation (once services exist, wire in tracing)
7. Docker Compose (containerize everything, confirm `docker compose up` works clean)
8. GitHub Actions CI/CD
9. Locust load testing + record real numbers
10. Write final README + resume bullets with real metrics filled in

---

## Tech Stack Summary

| Concern | Technology |
|---|---|
| API framework | FastAPI |
| Messaging | Redis Streams (via `redis-py`, `XADD`/`XREADGROUP`) |
| Database | PostgreSQL |
| Tracing | OpenTelemetry + Jaeger |
| Containerization | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| Load testing | Locust |
| Language | Python 3.11+ |

---

## Open Decisions (fill in before/while building)

- [ ] Repo name / GitHub repo created
- [ ] Confirm PostgreSQL schema (orders table: order_id, status, payload, created_at, updated_at, etc.)
- [ ] Confirm chaos/failure simulation approach for testing retries
- [ ] Decide whether to deploy anywhere real (Render/Fly.io/Railway free tier) or keep fully local
- [ ] Fill in real load test numbers once Step 9 is complete

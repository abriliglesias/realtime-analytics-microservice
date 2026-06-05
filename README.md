# Real-Time Analytics Microservice

A production-grade, event-driven analytics pipeline built with **Go**, **Apache Kafka**, **PostgreSQL**, and **Docker**. This system ingests high-throughput user activity events via a REST API, streams them through Kafka, and persists aggregated metrics atomically to a relational database — all without any local toolchain dependencies.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Key Engineering Decisions](#key-engineering-decisions)
  - [Aggregation-First Database Strategy](#1-aggregation-first-database-strategy)
  - [Horizontal Scalability via Kafka Partitions & Worker Pool](#2-horizontal-scalability-via-kafka-partitions--goroutine-worker-pool)
  - [Dependency Injection & Testable Architecture](#3-dependency-injection--testable-architecture)
  - [Dead-Letter Queue](#4-dead-letter-queue-dlq)
  - [Observability — Structured Logging & Trace IDs](#5-observability--structured-logging--trace-ids)
  - [Strict Input Validation](#6-strict-input-validation)
  - [Graceful Shutdown & Retry Logic](#7-graceful-shutdown--retry-logic)
  - [Bulletproof Health Checks](#8-bulletproof-health-checks)
  - [Zero-Dependency Developer Experience](#9-zero-dependency-developer-experience)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Makefile Reference](#makefile-reference)
- [API Reference](#api-reference)
- [Git Workflow](#git-workflow)

---

## Architecture Overview

```
┌─────────────────┐     HTTP POST      ┌──────────────────────┐
│   External       │ ─────────────────► │   Producer API       │
│   Client         │                    │   (Go REST, :8081)   │
└─────────────────┘                    └──────────┬───────────┘
                                                   │  Produces Message
                                                   ▼
                                        ┌──────────────────────┐
                                        │   Apache Kafka       │
                                        │   incoming.user_     │
                                        │   activity           │
                                        │   (3 partitions)     │
                                        └──────────┬───────────┘
                                                   │  Consumes (x3 Workers)
                                                   ▼
                                        ┌──────────────────────┐     permanent error
                                        │   Consumer Worker    │ ──────────────────►  incoming.user_activity.dlq
                                        │   (Goroutine Pool)   │
                                        └──────────┬───────────┘
                                                   │  UPSERT (atomic)
                                                   ▼
                                        ┌──────────────────────┐
                                        │   PostgreSQL         │
                                        │   user_metrics table │
                                        └──────────────────────┘
```

The system is composed of two independently deployable Go microservices that communicate exclusively through Kafka, ensuring full decoupling and independent scalability.

---

## Project Structure

```
analytics-project/
├── docker-compose.yml       # Orchestrates infra and Go apps
├── Makefile                 # Automates all DX tasks
├── database/                # Schema, queries, and sqlc config
├── producer/                # Go REST API + Dockerfile
└── consumer/                # Go Worker + Dockerfile
```

---

## Key Engineering Decisions

### 1. Aggregation-First Database Strategy

> **The problem with a raw event store:** Saving every raw `page_view` JSON event to a table creates unbounded growth. Querying total views for a user at read time would require an expensive `COUNT(*)` or `SUM()` across potentially millions of rows.

**The solution: Transform on write, not on read.**

Instead of a raw event log, the consumer performs an atomic `UPSERT` directly into a `user_metrics` table:

```sql
INSERT INTO user_metrics (user_id, page_view_count, last_active_at)
VALUES ($1, $2, NOW())
ON CONFLICT (user_id)
DO UPDATE SET
  page_view_count = user_metrics.page_view_count + EXCLUDED.page_view_count,
  last_active_at  = NOW();
```

**Why this matters:**

- **Atomic increments** — PostgreSQL handles the increment as a single atomic operation, preventing race conditions even under concurrent writes from multiple consumer workers.
- **O(1) reads** — Fetching a user's total page views is a direct primary-key lookup, not an aggregation scan. Reads are instantaneous regardless of event volume.
- **Bounded storage** — The table has exactly one row per user, not one row per event. Storage growth is proportional to your user base, not your traffic volume.
- **Type-safe queries via `sqlc`** — All SQL interactions are generated as idiomatic, type-safe Go code using `sqlc`, run via a temporary Docker container with zero local installation required.

---

### 2. Horizontal Scalability via Kafka Partitions & Goroutine Worker Pool

The system is designed to scale its throughput linearly by adding more consumer workers.

**Kafka Topic Configuration:**

The `incoming.user_activity` topic is created with **3 partitions**, enabling true parallelism:

```bash
kafka-topics --create \
  --topic incoming.user_activity \
  --partitions 3 \
  --replication-factor 1
```

**Goroutine Worker Pool (Consumer):**

The consumer launches 3 goroutine workers within the **same consumer group**. Kafka's consumer group protocol automatically assigns one partition per worker, meaning:

- Each worker reads from its own partition independently.
- No coordination overhead — workers never compete for the same messages.
- Throughput scales linearly: 3 partitions → 3x the processing capacity of a single consumer.

```
Consumer Group: analytics-consumer-group
  ├── Worker 1  ──► Partition 0
  ├── Worker 2  ──► Partition 1
  └── Worker 3  ──► Partition 2
```

To scale further, increase the partition count and deploy additional consumer instances — no code changes required.

---

### 3. Dependency Injection & Testable Architecture

Global variables in Go services are a common anti-pattern: they create hidden coupling, make testing difficult, and introduce subtle data races.

This project avoids them entirely by using **struct-based Dependency Injection**. HTTP handlers are methods on an `EventHandler` struct that holds its dependencies behind interfaces:

```go
// KafkaWriter is an interface — not the concrete *kafka.Writer type.
// This allows the handler to be tested by injecting a mock,
// with zero changes to production code.
type KafkaWriter interface {
    WriteMessages(ctx, ...kafka.Message) error
}

type EventHandler struct {
    writer KafkaWriter
    db     Querier
}

func NewEventHandler(writer KafkaWriter, db Querier) *EventHandler {
    return &EventHandler{writer: writer, db: db}
}
```

Unit tests inject a `mockKafkaWriter` that records every message written, enabling table-driven tests for all handler paths — valid payloads, missing fields, oversized bodies, and broker failures — without a live Kafka broker.

---

### 4. Dead-Letter Queue (DLQ)

The consumer distinguishes between two failure classes, each with a different resolution strategy:

| Error class | Example | Behaviour |
|---|---|---|
| **Permanent** | Malformed JSON, missing `user_id` | Routed to DLQ immediately — retrying will never help |
| **Transient** | DB connection timeout | Exponential backoff up to 5 retries, then DLQ |

Failed messages are published to `incoming.user_activity.dlq` with diagnostic headers attached:

```
trace_id        — correlates with the originating HTTP request
dlq_reason      — human-readable failure description
original_topic  — source topic for replay tooling
original_offset — exact Kafka offset of the failed message
failed_at       — RFC3339 timestamp
```

This means no message is ever silently dropped, and every failure is inspectable and replayable.

---

### 5. Observability — Structured Logging & Trace IDs

Every HTTP request is assigned a UUID `trace_id` by a middleware in the Producer API. The trace ID is:

1. Injected into the request `context.Context`
2. Set as an `X-Trace-ID` response header for client-side correlation
3. Embedded as a **Kafka message header** (not in the payload body) so it crosses the service boundary
4. Extracted by every Consumer worker and included in all subsequent log entries

The result is a correlated log chain across two services for every event:

```json
// Producer (request received)
{"level":"INFO","trace_id":"f47ac10b-58cc","msg":"request received","method":"POST","path":"/event"}

// Producer (enqueued)
{"level":"INFO","trace_id":"f47ac10b-58cc","msg":"event accepted and enqueued"}

// Consumer (persisted) — same trace_id, different process
{"level":"INFO","trace_id":"f47ac10b-58cc","msg":"event persisted","user_id":"user-abc","worker_id":2}
```

All logging uses Go's standard library `log/slog` with the JSON handler — no external logging dependency, machine-parseable output ready for any log aggregator.

---

### 6. Strict Input Validation

The `POST /event` endpoint applies three guards before a message touches Kafka:

```go
// Guard 1: Prevent memory exhaustion — enforced at the I/O layer,
// not via Content-Length (which can be spoofed or omitted).
r.Body = http.MaxBytesReader(w, r.Body, 1*1024*1024) // 1 MB hard limit

// Guard 2: Body must not be empty.
// Guard 3: Must be valid JSON with a non-empty user_id field.
if err := validateEvent(body); err != nil {
    http.Error(w, err.Error(), http.StatusBadRequest)
    return
}
```

Requests that fail validation receive a `400 Bad Request` with a descriptive message. Payloads exceeding 1 MB receive `413 Request Entity Too Large`. Neither path reaches the Kafka writer.

---

### 7. Graceful Shutdown & Retry Logic

Both microservices use `os/signal` to intercept `SIGTERM` and `SIGINT`, shutting down cleanly for container restarts and rolling deployments:

```go
// Shutdown sequence (Producer):
// 1. Stop accepting new HTTP connections (15s drain window)
// 2. Flush and close the Kafka writer
// 3. Close the PostgreSQL connection pool

// Shutdown sequence (Consumer):
// 1. Cancel the worker pool context — workers finish their current message
// 2. Close all Kafka readers (offsets committed)
// 3. Close the DLQ writer
```

The consumer also implements **exponential backoff** for transient DB failures, with each retry doubling the wait time up to a 30-second ceiling before routing to the DLQ.

---

### 8. Bulletproof Health Checks

Kafka has a well-known startup race condition: the JVM process and port 9092 become available before the broker is ready to accept client connections. A simple `depends_on` without a health check causes the Go services to fail on first boot.

The `docker-compose.yml` uses a native Kafka probe that performs a real metadata request:

```yaml
kafka:
  healthcheck:
    test: ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1 || exit 1"]
    interval: 15s
    timeout: 10s
    retries: 10
    start_period: 30s   # Give the JVM time to load before first probe

producer:
  depends_on:
    kafka:
      condition: service_healthy   # NOT service_started
    postgres:
      condition: service_healthy
```

This probe only passes when the broker can actually service metadata requests — not just when the port is open.

---

### 9. Zero-Dependency Developer Experience

The entire stack runs with nothing installed on the host except **Docker**, **make**, and (for local development only) **Go**.

- **Multi-Stage Dockerfiles** — A `golang:alpine` builder compiles the binary; a bare `alpine` image ships it. Production images contain no build toolchain.
- **Dockerized `sqlc`** — Code generation runs via `docker run --rm sqlc/sqlc:1.26.0`, pinning the version and requiring nothing locally.
- **Cross-platform Makefile** — All targets work on Linux, macOS, and Windows (via Git Bash or WSL). `SHELL := /bin/bash` with `-euo pipefail` ensures failures are loud, not silent.
- **`.env`-based configuration** — All ports, credentials, and topic names live in `.env` (copied from `.env.example`). No hardcoded values anywhere.

---

## Tech Stack

| Layer | Technology | Rationale |
|---|---|---|
| Language | Go | Performance, strong concurrency primitives, small binaries |
| Message Broker | Apache Kafka | Durable, partitioned, replay-capable event streaming |
| Database | PostgreSQL | ACID guarantees, `ON CONFLICT` UPSERT, excellent Go support |
| Query Generation | sqlc | Compile-time type safety for SQL queries |
| Containerization | Docker + Compose | Reproducible environments, zero host dependencies |
| Automation | Makefile | Cross-platform, self-documenting DX interface |

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) + Docker Compose
- `make`
- Go 1.21+ *(only required for `make run-producer`, `make run-consumer`, and `make test` — all other targets run inside Docker)*

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env if you need to change ports or credentials
```

### 2. Start the full stack

```bash
make up
```

Builds the Go service images and starts Kafka, Zookeeper, PostgreSQL, the Producer API, and the Consumer Worker in detached mode.

### 3. Create Kafka topics

```bash
make create-topic
```

Creates `incoming.user_activity` (3 partitions) and `incoming.user_activity.dlq` (1 partition). Idempotent — safe to re-run.

### 4. Initialize the database schema

```bash
make init-db
```

Polls until Postgres is ready, then runs migrations. Idempotent — safe to re-run.

### 5. Send a test event

```bash
make test-api
```

Fires a `curl` request to the Producer API and traces the event through Kafka into the database.

### 6. Query the aggregated metrics

```bash
curl "http://localhost:8081/metrics?user_id=user_123"
```

Returns the pre-aggregated `page_view_count` and `last_active_at` for the user — an O(1) primary-key lookup.

### Teardown

```bash
make down
```

Stops and removes all containers and volumes.

---

## Makefile Reference

| Command | Description |
|---|---|
| `make up` | Build images and start all containers (detached) |
| `make down` | Stop and remove all containers and volumes |
| `make logs` | Tail logs from all services |
| `make logs-<service>` | Tail logs for one service, e.g. `make logs-producer` |
| `make status` | Show running container state |
| `make init-db` | Run database migrations (idempotent) |
| `make create-topic` | Create Kafka topics — main + DLQ (idempotent) |
| `make generate` | Run `sqlc generate` via temporary Docker container |
| `make sqlc` | Alias for `make generate` |
| `make test` | Run all unit tests with race detector (`-race`) |
| `make test-api` | Send a sample `curl` request to smoke-test the pipeline |
| `make test-load` | Run the concurrent load test |
| `make run-producer` | Run the Producer API locally outside Docker |
| `make run-consumer` | Run the Consumer worker locally outside Docker |
| `make check-deps` | Verify Docker and Compose are available |

> Run `make help` for the full self-documenting reference with section groupings.

---

## API Reference

### `POST /event`

Ingests a user activity event, validates it, and publishes it to Kafka.

**Request Body:**

```json
{
  "user_id": "user-abc-123",
  "activity_type": "page_view",
  "metadata": {
    "page_url": "https://example.com"
  }
}
```

**Validation rules:** `user_id` is required and must be non-empty. Body must be valid JSON and under 1 MB.

**Responses:**

| Status | Meaning |
|---|---|
| `202 Accepted` | Event enqueued — not yet persisted (async by design) |
| `400 Bad Request` | Missing `user_id`, empty body, or invalid JSON |
| `413 Request Entity Too Large` | Body exceeds 1 MB |
| `500 Internal Server Error` | Kafka broker unreachable |

**Response header:** `X-Trace-ID: <uuid>` — use this to correlate logs end-to-end.

---

### `GET /metrics?user_id=X`

Returns pre-aggregated metrics for a user. This is an O(1) primary-key lookup — no on-the-fly aggregation.

**Example:**

```bash
curl "http://localhost:8081/metrics?user_id=user-abc-123"
```

**Response `200 OK`:**

```json
{
  "user_id": "user-abc-123",
  "page_view_count": 42,
  "last_active_at": "2026-06-03T14:22:11Z"
}
```

**Responses:**

| Status | Meaning |
|---|---|
| `200 OK` | Metrics returned |
| `400 Bad Request` | `user_id` query parameter missing |
| `404 Not Found` | No events recorded for this user |
| `503 Service Unavailable` | Database not initialised |

---

## Git Workflow

This project follows a clean, professional branching strategy to demonstrate production-level Git hygiene.

**Branch Strategy:**

| Branch | Purpose |
|---|---|
| `main` | Pristine, stable, always-deployable state |
| `feat/infrastructure` | Docker Compose, Makefile, base project scaffolding |
| `feat/kafka-producer` | Kafka topic setup and producer configuration |
| `feat/producer-api` | Go REST API for event ingestion |
| `feat/database-and-consumer` | PostgreSQL schema, sqlc queries, and consumer worker |

**Commit Convention:**

All commits follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add goroutine worker pool to consumer
fix: handle kafka writer timeout on shutdown
chore: add sqlc config and generate initial queries
docs: add architecture overview to README
```

This convention makes the project history machine-readable, enables automated changelog generation, and signals engineering maturity to reviewers.

---

## License

MIT
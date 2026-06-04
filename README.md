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
  - [Graceful Shutdown](#4-graceful-shutdown-bonus)
  - [Zero-Dependency Developer Experience](#5-zero-dependency-developer-experience)
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
│   Client         │                    │   (Go REST, :8080)   │
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
                                        ┌──────────────────────┐
                                        │   Consumer Worker    │
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
INSERT INTO user_metrics (user_id, page_view_count)
VALUES ($1, $2)
ON CONFLICT (user_id)
DO UPDATE SET
  page_view_count = user_metrics.page_view_count + EXCLUDED.page_view_count;
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
kafka-topics.sh --create \
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

This project avoids them entirely by using **struct-based Dependency Injection**. HTTP handlers are methods on an `EventHandler` struct that holds its dependencies explicitly:

```go
type EventHandler struct {
    writer *kafka.Writer
}

func NewEventHandler(writer *kafka.Writer) *EventHandler {
    return &EventHandler{writer: writer}
}

func (h *EventHandler) HandleEvent(w http.ResponseWriter, r *http.Request) {
    // h.writer is an injected, mockable dependency
}
```

**Benefits:**

- **Testability** — The Kafka writer can be swapped for a mock in unit tests without touching production code.
- **No data races** — Dependencies are initialized once and passed explicitly.
- **Extensibility** — Adding a new dependency (e.g., a metrics client) is a one-line change to the struct.

---

### 4. Graceful Shutdown 

Both microservices implement OS signal handling to shut down cleanly, preventing data loss or corrupted state during container restarts and rolling deployments.

```go
quit := make(chan os.Signal, 1)
signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
<-quit

// Shutdown sequence:
// 1. Stop accepting new HTTP connections (producer)
// 2. Flush and close the Kafka writer/reader
// 3. Close the PostgreSQL connection pool (consumer)
```

The consumer also implements **exponential backoff retry logic** for database writes, ensuring transient connectivity issues (e.g., a PostgreSQL container restart) do not result in dropped messages.

---

### 5. Zero-Dependency Developer Experience

The entire project runs with nothing installed on the host machine except **Docker and Make**.

- **Multi-Stage Dockerfiles** — Each Go service uses a multi-stage build: a `golang:alpine` builder compiles the binary, and a minimal `alpine` image ships it. This keeps production images lean and free of build toolchain bloat.
- **Dockerized tooling** — `sqlc` code generation and database migrations run inside temporary `golang:alpine` containers (`docker run --rm`). No Go, no `sqlc`, no `migrate` binary needed locally.
- **`docker-compose.yml`** — A single file orchestrates Kafka, Zookeeper, PostgreSQL, the Producer API, and the Consumer Worker with correct dependency ordering and health checks.

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

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- `make`

That's it. No Go installation. No local database. No Kafka client tools.

### 1. Start the full stack

```bash
make up
```

This builds the Go service images and starts Kafka, Zookeeper, PostgreSQL, the Producer API, and the Consumer Worker in detached mode.

### 2. Initialize the database schema

```bash
make init-db
```

Runs migrations inside a temporary Docker container against the running PostgreSQL instance.

### 3. Create the partitioned Kafka topic

```bash
make create-topic
```

Creates `incoming.user_activity` with 3 partitions inside the Kafka container.

### 4. Send a test event

```bash
make test-api
```

Fires a `curl` request to the Producer API and traces the event through Kafka into the database.

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
| `make init-db` | Run database migrations via temporary Docker container |
| `make create-topic` | Create the `incoming.user_activity` Kafka topic (3 partitions) |
| `make generate` | Run `sqlc generate` via temporary Docker container |
| `make test-api` | Send a sample `curl` request to the Producer API |
| `make logs` | Tail logs from all running services |

---

## API Reference

### `POST /event`

Ingests a user activity event and publishes it to Kafka.

**Request Body:**

```json
{
  "user_id": "user-abc-123",
  "event_type": "page_view",
  "metadata": {
    "page": "/home"
  }
}
```

**Response:**

```
202 Accepted
```

The `202` status intentionally signals that the event has been accepted and enqueued — not that it has been persisted. This reflects the async nature of the pipeline.

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

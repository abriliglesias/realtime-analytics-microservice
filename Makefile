# =============================================================================
#  Real-Time Analytics Microservice — Makefile
#
#  Usage:      make <target>
#  Commands:   make help
#
#  Platform requirements
#  ─────────────────────
#  Linux / macOS : bash ≥ 3.x (pre-installed everywhere)
#  Windows       : Git Bash or WSL — NOT native PowerShell/cmd.
#                  Both ship bash and make this file work without changes.
#
#  The only required tools on the host are Docker and make.
#  Go, sqlc, psql, and kafka-topics all run inside Docker containers.
# =============================================================================

SHELL        := /bin/bash
.SHELLFLAGS  := -euo pipefail -c

# =============================================================================
#  .env loading
#  ─────────────
#  1. Copy .env.example to .env and edit values before running anything.
#  2. The -include directive silently skips the include when .env is absent
#     (e.g. a fresh CI checkout), so CI pipelines that inject variables via
#     secrets still work without a committed .env file.
#  3. `export` re-exports every variable so child processes (docker, go, curl)
#     inherit them automatically — no explicit -e flags on every docker run.
# =============================================================================
-include .env
export

# ── Defaults ────────
PRODUCER_PORT             ?= 8081
PRODUCER_PORT_LOC         ?= 8080
KAFKA_BROKERS             ?= localhost:9092
KAFKA_TOPIC               ?= incoming.user_activity
KAFKA_DLQ_TOPIC           ?= incoming.user_activity.dlq
KAFKA_PARTITIONS          ?= 3
KAFKA_REPLICATION_FACTOR  ?= 1
POSTGRES_USER             ?= user
POSTGRES_PASSWORD         ?= password
POSTGRES_DB               ?= analytics
DATABASE_URL              ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:5432/$(POSTGRES_DB)?sslmode=disable
GO_IMAGE                  ?= golang:1.23-alpine
SQLC_IMAGE                ?= sqlc/sqlc:1.26.0

# ── Internal shorthands ───────────────────────────────────────────────────────
DC    := docker compose
# -T: disable pseudo-TTY (prevents "not a TTY" errors in CI and on Windows)
KAFKA := $(DC) exec -T kafka kafka-topics --bootstrap-server localhost:9092

# =============================================================================
.PHONY: help \
        check-docker check-go check-deps \
        up down logs status \
        run-producer run-consumer \
        sqlc generate \
        init-db \
        create-topic \
        test test-api test-load
# =============================================================================

# =============================================================================
#  HELP — auto-generated from ## comments
# =============================================================================

help: ## Show this help
	@echo ""
	@echo "  Real-Time Analytics Microservice"
	@echo "  ════════════════════════════════"
	@awk 'BEGIN { FS = ":.*##"; section="" } \
	    /^##@/ { \
	        section=$$0; gsub(/^##@ */, "", section); \
	        printf "\n  \033[1;34m%s\033[0m\n", section \
	    } \
	    /^[a-zA-Z0-9_-]+:.*##/ { \
	        printf "    \033[36m%-20s\033[0m %s\n", $$1, $$2 \
	    }' $(MAKEFILE_LIST)
	@echo ""

# =============================================================================
##@ Setup / Infrastructure
# =============================================================================
check-docker: ## Verify Docker daemon is running and Compose v2 is available
	@command -v docker >/dev/null 2>&1 \
	    || { echo "docker not found — install Docker Desktop or Docker Engine"; exit 1; }
	@docker info >/dev/null 2>&1 \
	    || { echo "Docker daemon is not running — start Docker and retry"; exit 1; }
	@docker compose version >/dev/null 2>&1 \
	    || { echo "docker compose (v2) not found — upgrade Docker Desktop"; exit 1; }
	@echo "Docker available"
 
check-go: ## Verify Go is installed (only needed for run-producer / run-consumer / test)
	@command -v go >/dev/null 2>&1 \
	    || { echo "go not found — install from https://go.dev/dl or use 'make up' instead"; exit 1; }
	@echo "$$(go version)"

check-deps: check-docker ## Run all dependency checks
	@echo "All required dependencies present"

up: check-docker ## Build images and start all containers in detached mode
	@echo "Building and starting all containers..."
	$(DC) up --build -d
	@echo ""
	@echo "  Services started.  Useful next steps:"
	@echo "    make create-topic   — create Kafka topics"
	@echo "    make init-db        — run database migrations"
	@echo "    make test-api       — smoke-test the full pipeline"
	@echo "    make logs           — tail all service logs"
	@echo ""

down: check-docker ## Stop and remove all containers and volumes
	$(DC) down -v

logs: ## Tail logs from all services  (Ctrl-C to exit)
	$(DC) logs -f

logs-%: ## Tail logs for one service, e.g.  make logs-producer
	$(DC) logs -f $*

status: ## Show running container state
	$(DC) ps

# =============================================================================
##@ Development / Run
# =============================================================================

run-producer: check-go ## Run the Producer API locally (outside Docker)
	@echo "Starting Producer API locally on port $(PRODUCER_PORT_LOC)..."
	cd producer && export SERVER_PORT=$(PRODUCER_PORT_LOC) KAFKA_BROKERS=$(KAFKA_BROKERS) && go run ./...

run-consumer: check-go ## Run the Consumer worker locally (outside Docker)
	@echo "Starting Consumer locally..."
	cd consumer && export SERVER_PORT=$(PRODUCER_PORT_LOC) KAFKA_BROKERS=$(KAFKA_BROKERS) && go run ./...

# =============================================================================
##@ Database
# =============================================================================

init-db: check-docker ## Run database migrations — idempotent, safe to re-run
	@echo "Waiting for Postgres to accept connections..."
	@until $(DC) exec -T postgres \
	    pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) >/dev/null 2>&1; do \
	        printf "."; sleep 2; \
	done
	@echo " ready."
	@echo "Running migrations..."
	cd database && go run migrate.go

sqlc: check-docker ## Re-generate type-safe Go code from SQL 
	@$(MAKE) --no-print-directory generate

generate: check-docker ## Re-generate type-safe Go code from SQL via Docker (no local sqlc needed)
	@echo "Generating sqlc code..."
	@MSYS_NO_PATHCONV=1 docker run --rm \
	    -v "$(CURDIR)/database:/src" \
	    -w /src \
	    $(SQLC_IMAGE) generate
	@echo "sqlc generation complete"

# =============================================================================
##@ Kafka
# =============================================================================

create-topic: check-docker ## Create main + DLQ Kafka topics — idempotent, safe to re-run
# Polls until the broker is genuinely ready (not just port-open) before
# issuing commands. --if-not-exists makes re-runs silent no-ops.
	@echo "Waiting for Kafka broker to be ready..."
	@until $(KAFKA) --list >/dev/null 2>&1; do \
	    printf "."; sleep 3; \
	done
	@echo " ready."
	$(KAFKA) --create --if-not-exists \
	    --topic $(KAFKA_TOPIC) \
	    --partitions $(KAFKA_PARTITIONS) \
	    --replication-factor $(KAFKA_REPLICATION_FACTOR)
	$(KAFKA) --create --if-not-exists \
	    --topic $(KAFKA_DLQ_TOPIC) \
	    --partitions 1 \
	    --replication-factor $(KAFKA_REPLICATION_FACTOR)
	@echo "Topics ready (created or already existed)"
	@echo "Restarting consumer to pick up new topics..."
    $(DC) restart consumer

# =============================================================================
##@ Testing / Quality
# =============================================================================

test: check-go ## Run all unit tests with race detector
	@echo "── Producer tests ───────────────────────────────────"
	cd producer && CGO_ENABLED=1 go test -v -race ./...
	@echo ""
	@echo "── Consumer tests ───────────────────────────────────"
	cd consumer && CGO_ENABLED=1 go test -v -race ./...


test-api: ## Smoke-test the full pipeline: POST /event → Kafka → Consumer → Postgres
	@echo "── Sending test event ───────────────────────────────"
	curl -i -X POST http://localhost:$(PRODUCER_PORT)/event \
	    -H "Content-Type: application/json" \
	    -d '{"user_id":"user_123","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z","metadata":{"page_url":"https://example.com"}}'
	@echo ""

test-load: check-go ## Run the concurrent load test
	@echo "Running concurrent load test..."
	@test -f load.go \
	    || { echo "load.go not found in repo root — check its location and update this target"; exit 1; }
	go run load.go
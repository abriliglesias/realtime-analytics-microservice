PRODUCER_PORT ?= 8081
KAFKA_BROKERS ?= localhost:9092

ifeq ($(OS),Windows_NT)
  PRODUCER_ENV = cmd /c "set SERVER_PORT=$(PRODUCER_PORT)&& set KAFKA_BROKERS=$(KAFKA_BROKERS)&& go run ."
else
  PRODUCER_ENV = SERVER_PORT=$(PRODUCER_PORT) KAFKA_BROKERS=$(KAFKA_BROKERS) go run .
endif

.PHONY: up down sqlc run-producer run-consumer test-api create-topic init-db test-load

# Start everything (Infrastructure + Go Microservices)
up:
	@echo "Building and starting all containers..."
	docker-compose up --build -d

# Stop everything
down:
	docker-compose down -v

# Generate the type-safe Go code from your SQL files
#@echo "Installing sqlc (if not present)..."
#	go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
#"$$(go env GOPATH)/bin/sqlc" generate
sqlc:
	@echo "Generating sqlc code..."
	cd database && sqlc generate

# Run the Producer locally
run-producer:
	@echo "Starting Producer API locally on port $(PRODUCER_PORT)..."
	cd producer && $(PRODUCER_ENV)

# Run the Producer in Docker via Docker Compose
run-producer-docker:
	@echo "Starting producer in Docker Compose..."
	docker-compose up --build -d kafka zookeeper producer

# Run the Consumer locally
run-consumer:
	cd consumer && go run .

# Run the Consumer in Docker via Docker Compose
run-consumer-docker:
	@echo "Starting consumer in Docker Compose..."
	docker-compose up --build -d kafka zookeeper postgres consumer

# --- Test the Producer API ---
test-api:
	@echo "Sending test event to the API..."
	curl -i -X POST http://localhost:$(PRODUCER_PORT)/events \
	  -H "Content-Type: application/json" \
	  -d '{"user_id": "user_123", "activity_type": "page_view", "timestamp": "2024-07-01T12:34:56Z", "metadata": {"page_url": "https://example.com"}}'

# --- Create Kafka Topic with Partitions ---
create-topic:
	@echo "Creating Kafka topic with 3 partitions..."
	docker exec analytics-kafka kafka-topics --create --if-not-exists --topic incoming.user_activity --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# --- Initialize the Database (Run Migrations) ---
init-db:
	@echo "Initializing the database (running migrations)..."
	cd database && go run migrate.go

# --- Run Concurrent Load Test ---
test-load:
	@echo "Running concurrent load test locally..."
	go run load.go
	
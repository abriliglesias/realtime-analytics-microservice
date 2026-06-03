.PHONY: up down sqlc run-producer run-consumer test-api create-topic init-db

# Start the infrastructure (Kafka, Zookeeper, Postgres)
up:
	docker-compose up -d postgres zookeeper kafka

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
	cd producer && go run .

# Run the Consumer locally
run-consumer:
	cd consumer && go run .

# --- Test the Producer API ---
test-api:
	@echo "Sending test event to the API..."
	curl -i -X POST http://localhost:8080/events \
	  -H "Content-Type: application/json" \
	  -d '{"user_id": "user_123", "activity_type": "page_view", "timestamp": "2024-07-01T12:34:56Z", "metadata": {"page_url": "https://example.com"}}'

# --- Create Kafka Topic with Partitions ---
create-topic:
	@echo "Creating Kafka topic with 3 partitions..."
	-docker exec analytics-kafka kafka-topics --create --topic incoming.user_activity --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# --- Initialize the Database (Run Migrations) ---
init-db:
	@echo "Initializing the database (running migrations)..."
	cd database && go run migrate.go
	
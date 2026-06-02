.PHONY: up down sqlc run-producer run-consumer test-api

# Start the infrastructure (Kafka, Zookeeper, Postgres)
up:
	docker-compose up -d postgres zookeeper kafka

# Stop everything
down:
	docker-compose down -v

# Generate the type-safe Go code from your SQL files
sqlc:
	@echo "Generating sqlc code..."
	cd database && sqlc generate

# Run the Producer locally
run-producer:
	cd producer && go run .

# Run the Consumer locally
run-consumer:
	cd consumer && go run main.go

# --- NEW: Test the Producer API ---
test-api:
	@echo "Sending test event to the API..."
	curl -i -X POST http://localhost:8080/events \
	  -H "Content-Type: application/json" \
	  -d '{"user_id": "user_123", "activity_type": "page_view", "timestamp": "2024-07-01T12:34:56Z", "metadata": {"page_url": "https://example.com"}}'
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"analytics-consumer/internal/db" // Adjust this if your go.mod name is different
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func main() {
	// 1. Setup Database Connection using pgxpool
    dbURL := os.Getenv("DATABASE_URL")
    if dbURL == "" {
        dbURL = "postgres://analytics_user:analytics_password@localhost:5432/analytics_db"
    }

    // pgxpool handles connection pooling automatically
    pool, err := pgxpool.New(context.Background(), dbURL)
    if err != nil {
        log.Fatalf("Could not connect to database: %v", err)
    }
    defer pool.Close()

    queries := db.New(db.NewQuerier(pool))
    log.Println("Connected to PostgreSQL successfully.")

	// 2. Setup Kafka Reader
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "incoming.user_activity",
		GroupID:  "analytics-consumer-group",
		MaxBytes: 10e6,
	})
	defer reader.Close()

	// 3. Initialize the Processor Struct (Dependency Injection)
	processor := &EventProcessor{
		queries: queries,
		reader:  reader,
	}

	// 4. Graceful Shutdown & Concurrency Setup
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// Launch 3 concurrent workers via the struct method
	processor.StartWorkers(ctx, &wg, 3)

	// 5. Wait for shutdown signal
	<-stopChan
	log.Println("Termination signal received. Shutting down gracefully...")
	cancel()  // This tells all workers to stop reading
	wg.Wait() // Wait for all workers to finish their current task

	log.Println("Consumer exited cleanly.")
}

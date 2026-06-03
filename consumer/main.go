package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"analytics-consumer/internal/db"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Initialize Database Connection
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://analytics_user:analytics_password@localhost:5432/analytics_db?sslmode=disable"
	}

	// Call the cleaner helper function
	dbConn, err := InitDB(dbURL)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer dbConn.Close()

	queries := db.New(dbConn)

	// Setup Kafka Reader
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

	// Initialize the Processor Struct (Dependency Injection)
	processor := &EventProcessor{
		queries: queries,
		reader:  reader,
	}

	// Graceful Shutdown & Concurrency Setup
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// Launch 3 concurrent workers via the struct method
	processor.StartWorkers(ctx, &wg, 3)

	// Wait for shutdown signal
	<-stopChan
	log.Println("Termination signal received. Shutting down gracefully...")
	cancel()  // This tells all workers to stop reading
	wg.Wait() // Wait for all workers to finish their current task

	log.Println("Consumer exited cleanly.")
}

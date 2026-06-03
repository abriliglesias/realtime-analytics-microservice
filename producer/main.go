package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092" // Default for local testing outside Docker
	}

	// Initialize the Kafka Writer
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  "incoming.user_activity",
		Balancer:               &kafka.LeastBytes{}, // Distributes messages evenly across partitions
		AllowAutoTopicCreation: true,
	}

	// Inject the Writer into our Handler
	eventHandler := &EventHandler{
		kafkaWriter: kafkaWriter,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", eventHandler.handleIncomingEvent)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// --- Graceful Shutdown Setup ---
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Starting Producer API on port %s...", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v\n", err)
		}
	}()

	<-stopChan
	log.Println("Termination signal received. Shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited cleanly")
}

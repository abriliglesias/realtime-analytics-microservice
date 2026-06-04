package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func main() {
	// --- Structured JSON logging (replaces log.Printf everywhere) ---
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// --- Kafka writer ---
	kafkaBroker := getEnv("KAFKA_BROKERS", "kafka:9092")
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        "incoming.user_activity",
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}

	// --- PostgreSQL pool (required for GET /metrics) ---
	dbURL := getEnv("DATABASE_URL", "postgres://user:password@postgres:5432/analytics?sslmode=disable")
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		slog.Error("failed to connect to postgres", slog.Any("error", err))
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		slog.Error("postgres ping failed", slog.Any("error", err))
		os.Exit(1)
	}
	slog.Info("connected to postgres")

	// Wrap the pgxpool with the sqlc-generated Queries type.
	// Uncomment once your database package is in place:
	//   queries := db.New(pool)
	//   handler := NewEventHandler(writer, queries)
	//
	// For now, pass nil to keep compilation clean:
	handler := NewEventHandler(writer, nil)

	// --- Router ---
	mux := http.NewServeMux()
	mux.HandleFunc("POST /event", handler.HandleEvent)
	mux.HandleFunc("GET /metrics", handler.HandleGetMetrics)

	// Wrap all routes in the trace middleware (generates + propagates trace_id).
	root := TraceMiddleware(mux)

	serverPort := getEnv("SERVER_PORT", "8080")
	server := &http.Server{
		Addr:         ":" + serverPort,
		Handler:      root,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// --- Graceful shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("producer API listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("http server shutdown error", slog.Any("error", err))
	}

	if err := writer.Close(); err != nil {
		slog.Error("kafka writer close error", slog.Any("error", err))
	}

	slog.Info("producer shut down cleanly")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

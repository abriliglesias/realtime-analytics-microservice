package main

import (
	"analytics-consumer/internal/db"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

const (
	topic         = "incoming.user_activity"
	dlqTopic      = "incoming.user_activity.dlq"
	consumerGroup = "analytics-consumer-group"
	numWorkers    = 3

	maxRetries  = 5
	baseBackoff = 200 * time.Millisecond
	maxBackoff  = 30 * time.Second
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")

	// --- DLQ writer ---
	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}

	// --- DB pool ---
	// FIX 1: getEnv provides a fallback so an unset DATABASE_URL produces a
	// clear fatal error ("cannot be empty") instead of a cryptic pgx message.
	dbURL := getEnv("DATABASE_URL", "")
	if dbURL == "" {
		slog.Error("DATABASE_URL environment variable is not set — cannot connect to PostgreSQL")
		os.Exit(1)
	}

	// FIX 2: The error from pgxpool.New was previously discarded with _.
	// If the connection string is malformed or the DB is unreachable at parse
	// time, pool is nil and every subsequent query silently fails (or panics).
	// Now we treat this as a fatal startup error.
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		slog.Error("failed to create PostgreSQL connection pool",
			slog.String("database_url", dbURL),
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	// FIX 3: Ping verifies the pool can actually reach the database, not just
	// that the connection string parsed successfully. Without this, a wrong
	// password or a DB that isn't ready yet produces no error until the first
	// query fires inside a worker — at which point the error is swallowed by
	// writeWithRetry and messages silently drain into the DLQ.
	if err := pool.Ping(context.Background()); err != nil {
		slog.Error("PostgreSQL ping failed — is the database running and reachable?",
			slog.String("database_url", dbURL),
			slog.Any("error", err),
		)
		os.Exit(1)
	}
	slog.Info("connected to PostgreSQL")
	defer pool.Close()

	queries := db.New(pool)

	// --- Graceful shutdown ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		runWorkerPool(ctx, kafkaBroker, dlqWriter, queries)
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received, draining workers...")
	<-done

	if err := dlqWriter.Close(); err != nil {
		slog.Error("dlq writer close error", slog.Any("error", err))
	}
	slog.Info("consumer shut down cleanly")
}

// -------------------------------------------------------------------
// Worker pool
// -------------------------------------------------------------------

func runWorkerPool(ctx context.Context, broker string, dlqWriter *kafka.Writer, queries *db.Queries) {
	workerDone := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer func() { workerDone <- struct{}{} }()
			runWorker(ctx, workerID, broker, dlqWriter, queries)
		}(i)
	}

	for i := 0; i < numWorkers; i++ {
		<-workerDone
	}
}

func runWorker(ctx context.Context, workerID int, broker string, dlqWriter *kafka.Writer, queries *db.Queries) {
	log := slog.With(slog.Int("worker_id", workerID))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        consumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error("kafka reader close error", slog.Any("error", err))
		}
	}()

	log.Info("worker started")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("worker context cancelled, exiting")
				return
			}
			log.Error("kafka read error", slog.Any("error", err))
			continue
		}

		traceID := extractTraceID(msg)
		msgLog := log.With(
			slog.String("trace_id", traceID),
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)

		processMessage(ctx, msg, traceID, msgLog, dlqWriter, queries)
	}
}

// -------------------------------------------------------------------
// Message processing
// -------------------------------------------------------------------

func processMessage(
	ctx context.Context,
	msg kafka.Message,
	traceID string,
	log *slog.Logger,
	dlqWriter *kafka.Writer,
	queries *db.Queries,
) {
	var event UserActivity
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Warn("malformed message, routing to DLQ",
			slog.String("reason", err.Error()),
			slog.String("raw_value", string(msg.Value)),
		)
		sendToDLQ(ctx, dlqWriter, msg, traceID, "unmarshal_error: "+err.Error(), log)
		return
	}

	if event.UserID == "" {
		log.Warn("message missing user_id, routing to DLQ",
			slog.String("raw_value", string(msg.Value)),
		)
		sendToDLQ(ctx, dlqWriter, msg, traceID, "missing_user_id", log)
		return
	}

	if err := writeWithRetry(ctx, event, traceID, log, queries); err != nil {
		log.Error("all retries exhausted, routing to DLQ",
			slog.String("user_id", event.UserID),
			slog.Any("error", err),
		)
		sendToDLQ(ctx, dlqWriter, msg, traceID, "db_error_after_retries: "+err.Error(), log)
	}
}

// -------------------------------------------------------------------
// DB write with exponential backoff
// -------------------------------------------------------------------

func writeWithRetry(ctx context.Context, event UserActivity, traceID string, log *slog.Logger, queries *db.Queries) error {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := calculateBackoff(attempt)
			log.Info("retrying db write",
				slog.String("trace_id", traceID),
				slog.String("user_id", event.UserID),
				slog.Int("attempt", attempt),
				slog.Duration("backoff", backoff),
			)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := queries.UpsertUserMetrics(ctx, db.UpsertUserMetricsParams{
			UserID:        event.UserID,
			PageViewCount: 1,
			LastActiveAt:  pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
		})
		if err == nil {
			log.Info("event persisted",
				slog.String("trace_id", traceID),
				slog.String("user_id", event.UserID),
				slog.String("activity_type", event.ActivityType),
			)
			return nil
		}

		lastErr = err
		log.Warn("db write failed",
			slog.String("trace_id", traceID),
			slog.String("user_id", event.UserID),
			slog.Int("attempt", attempt+1),
			slog.Any("error", err),
		)
	}

	return lastErr
}

func calculateBackoff(attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt))
	d := time.Duration(float64(baseBackoff) * exp)
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

// -------------------------------------------------------------------
// DLQ publisher
// -------------------------------------------------------------------

func sendToDLQ(
	ctx context.Context,
	dlqWriter *kafka.Writer,
	original kafka.Message,
	traceID string,
	reason string,
	log *slog.Logger,
) {
	dlqMsg := kafka.Message{
		Value: original.Value,
		Headers: []kafka.Header{
			{Key: "trace_id", Value: []byte(traceID)},
			{Key: "dlq_reason", Value: []byte(reason)},
			{Key: "original_topic", Value: []byte(topic)},
			{Key: "original_offset", Value: []byte(formatInt64(original.Offset))},
			{Key: "failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	if err := dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
		log.Error("CRITICAL: failed to write to DLQ",
			slog.String("trace_id", traceID),
			slog.String("dlq_reason", reason),
			slog.Any("error", err),
		)
		return
	}

	log.Warn("message sent to DLQ",
		slog.String("trace_id", traceID),
		slog.String("dlq_reason", reason),
	)
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------

func extractTraceID(msg kafka.Message) string {
	for _, h := range msg.Headers {
		if h.Key == "trace_id" {
			return string(h.Value)
		}
	}
	return "no-trace-id"
}

func formatInt64(n int64) string {
	return fmt.Sprintf("%d", n)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

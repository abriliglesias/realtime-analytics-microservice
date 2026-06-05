package main

import (
	"analytics/internal/db"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/segmentio/kafka-go"
)

const maxBodyBytes = 1 * 1024 * 1024 // 1 MB

// UserEvent is the expected JSON body for POST /event.
type UserEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
}

// KafkaWriter is an interface over kafka.Writer so the handler can be tested
// without a live Kafka broker. Only the single method we use is declared.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// EventHandler holds injected dependencies for the HTTP handlers.
type EventHandler struct {
	writer KafkaWriter
	db     *db.Queries
}

// NewEventHandler constructs an EventHandler with its required dependencies.
func NewEventHandler(writer KafkaWriter, queries *db.Queries) *EventHandler {
	return &EventHandler{writer: writer, db: queries}
}

// -------------------------------------------------------------------
// POST /event
// -------------------------------------------------------------------

// HandleEvent validates the incoming request, then publishes the raw JSON body
// to Kafka with the trace_id embedded as a message header.
func (h *EventHandler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	traceID := TraceIDFromContext(ctx)
	log := slog.With(slog.String("trace_id", traceID), slog.String("handler", "HandleEvent"))

	// --- 1. Guard against memory-exhaustion attacks ---
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// MaxBytesReader returns a specific error when the limit is exceeded.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			log.WarnContext(ctx, "request body too large")
			http.Error(w, "request body too large (max 1MB)", http.StatusRequestEntityTooLarge)
			return
		}
		log.ErrorContext(ctx, "failed to read body", slog.Any("error", err))
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	// --- 2. Structural validation ---
	if err := validateEvent(body); err != nil {
		log.WarnContext(ctx, "invalid event payload",
			slog.String("reason", err.Error()),
		)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// --- 3. Publish to Kafka with trace_id header ---
	msg := kafka.Message{
		Value: body,
		Headers: []kafka.Header{
			{Key: "trace_id", Value: []byte(traceID)},
		},
	}

	if err := h.writer.WriteMessages(ctx, msg); err != nil {
		log.ErrorContext(ctx, "failed to publish message to kafka",
			slog.Any("error", err),
		)
		http.Error(w, "failed to enqueue event", http.StatusInternalServerError)
		return
	}

	log.InfoContext(ctx, "event accepted and enqueued")
	w.WriteHeader(http.StatusAccepted)
}

// validateEvent performs all structural checks on the raw request body.
// Returning a descriptive error allows the caller to surface it directly
// to the client as a 400 response body.
func validateEvent(body []byte) error {
	if len(body) == 0 {
		return errors.New("request body must not be empty")
	}

	var event UserEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return errors.New("request body must be valid JSON")
	}

	if event.UserID == "" {
		return errors.New("field 'user_id' is required and must not be empty")
	}

	return nil
}

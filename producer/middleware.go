package main

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

// contextKey is an unexported type for context keys in this package,
// preventing collisions with keys defined in other packages.
type contextKey string

const traceIDKey contextKey = "trace_id"

// TraceMiddleware generates a unique trace_id UUID for every incoming HTTP
// request, injects it into the request context, and logs the request details.
// Downstream handlers retrieve it via TraceIDFromContext and embed it in
// Kafka message headers so the consumer can correlate the full event journey.
func TraceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := uuid.NewString()

		ctx := context.WithValue(r.Context(), traceIDKey, traceID)

		// Surface the trace ID in the response so callers can correlate logs.
		w.Header().Set("X-Trace-ID", traceID)

		slog.InfoContext(ctx, "request received",
			slog.String("trace_id", traceID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote_addr", r.RemoteAddr),
		)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TraceIDFromContext retrieves the trace_id from a context.
// Returns an empty string if no trace ID was set (e.g. in tests that bypass
// the middleware), so callers never panic on a missing value.
func TraceIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(traceIDKey).(string)
	return v
}

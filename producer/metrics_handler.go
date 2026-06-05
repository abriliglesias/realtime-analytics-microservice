package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// -------------------------------------------------------------------
// GET /metrics?user_id=X
// -------------------------------------------------------------------

// MetricsResponse is the JSON shape returned to callers.
type MetricsResponse struct {
	UserID       string    `json:"user_id"`
	PageViews    int64     `json:"page_view_count"`
	LastActiveAt time.Time `json:"last_active_at"`
}

// HandleGetMetrics handles GET /metrics?user_id=X.
// It reads the pre-aggregated row from PostgreSQL — a sub-millisecond primary
// key lookup — and returns it as JSON. Because the consumer writes via UPSERT
// this value is always current without any on-the-fly aggregation.
func (h *EventHandler) HandleGetMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	traceID := TraceIDFromContext(ctx)
	log := slog.With(
		slog.String("trace_id", traceID),
		slog.String("handler", "HandleGetMetrics"),
	)

	if h.db == nil {
		log.ErrorContext(ctx, "db querier is nil — read model not initialised")
		http.Error(w, "read model unavailable", http.StatusServiceUnavailable)
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		log.WarnContext(ctx, "missing user_id query parameter")
		http.Error(w, "query parameter 'user_id' is required", http.StatusBadRequest)
		return
	}

	metrics, err := h.db.GetUserMetrics(ctx, userID)
	if err != nil {
		// Distinguish "not found" from a real DB error so clients get the right
		// status code. pgx returns pgx.ErrNoRows;
		if isNotFound(err) {
			log.InfoContext(ctx, "user not found", slog.String("user_id", userID))
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}
		log.ErrorContext(ctx, "db query failed",
			slog.String("user_id", userID),
			slog.Any("error", err),
		)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	resp := MetricsResponse{
		UserID:       metrics.UserID,
		PageViews:    int64(metrics.PageViewCount),
		LastActiveAt: metrics.LastActiveAt.Time,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.ErrorContext(ctx, "failed to encode response", slog.Any("error", err))
	}

	log.InfoContext(ctx, "metrics served",
		slog.String("user_id", userID),
		slog.Int64("page_view_count", int64(metrics.PageViewCount)),
	)
}

func isNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

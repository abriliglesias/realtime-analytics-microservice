package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// UserActivity maps directly to the challenge's JSON payload
type UserActivity struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata"`
}

// EventHandler holds the dependencies needed by the HTTP handlers.
type EventHandler struct {
	// kafkaWriter *kafka.Writer // Placeholder for future Kafka integration
}

// handleIncomingEvent processes the POST request
func (h *EventHandler) handleIncomingEvent(w http.ResponseWriter, r *http.Request) {
	var activity UserActivity

	if err := json.NewDecoder(r.Body).Decode(&activity); err != nil {
		http.Error(w, `{"error": "Invalid JSON payload"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Structural Validation
	if activity.UserID == "" || activity.ActivityType == "" {
		http.Error(w, `{"error": "Missing required fields"}`, http.StatusBadRequest)
		return
	}

	// TODO: Integrate Kafka client here using h.kafkaWriter

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "event accepted"}`))
}

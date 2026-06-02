package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
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
	kafkaWriter *kafka.Writer // Kafka Writer for producing messages
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

	// 1. Serialize the validated struct back to JSON
	messageBytes, err := json.Marshal(activity)
	if err != nil {
		log.Printf("Error marshalling activity: %v", err)
		http.Error(w, `{"error": "Internal server error"}`, http.StatusInternalServerError)
		return
	}

	// 2. Publish to Kafka
	err = h.kafkaWriter.WriteMessages(r.Context(),
		kafka.Message{
			Key:   []byte(activity.UserID),
			Value: messageBytes,
		},
	)

	if err != nil {
		log.Printf("Failed to write message to Kafka: %v", err)
		http.Error(w, `{"error": "Failed to queue event"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "event accepted"}`))
}

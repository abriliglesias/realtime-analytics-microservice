package main

import (
	"time"

	"analytics/internal/db"

	"github.com/segmentio/kafka-go"
)

// UserActivity matches the JSON we send from the Producer
type UserActivity struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata"`
}

// EventProcessor holds the dependencies needed to process Kafka messages
type EventProcessor struct {
	queries *db.Queries
	reader  *kafka.Reader
}

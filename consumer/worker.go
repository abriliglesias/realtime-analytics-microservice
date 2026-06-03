package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"analytics-consumer/internal/db" // Adjust this if your go.mod name is different

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

// StartWorkers launches N concurrent goroutines to process messages
func (p *EventProcessor) StartWorkers(ctx context.Context, wg *sync.WaitGroup, numWorkers int) {
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go p.worker(ctx, wg, i)
	}
}

// worker is the internal method that actually reads and processes the data
func (p *EventProcessor) worker(ctx context.Context, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()
	log.Printf("Worker %d started", workerID)

	for {
		// ReadMessage blocks until a message is available or the context is cancelled
		msg, err := p.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context was cancelled (Graceful Shutdown)
				log.Printf("Worker %d shutting down", workerID)
				return
			}
			log.Printf("Worker %d error reading message: %v", workerID, err)
			continue // Retry logic
		}

		var activity UserActivity
		if err := json.Unmarshal(msg.Value, &activity); err != nil {
			log.Printf("Worker %d failed to parse JSON: %v", workerID, err)
			continue
		}

		// Transform data: Only increment page views if it's a "page_view" activity
		pageViewIncrement := int32(0)
		if activity.ActivityType == "page_view" {
			pageViewIncrement = 1
		}

		// Save to Database using the generated sqlc code
		err = p.queries.UpsertUserActivity(ctx, db.UpsertUserActivityParams{
			UserID:              activity.UserID,
			PageViewCount:       pageViewIncrement,
			LastActiveTimestamp: activity.Timestamp,
		})

		if err != nil {
			log.Printf("Worker %d failed to update database: %v", workerID, err)
			// Note: In a real system, you might implement exponential backoff here
		} else {
			log.Printf("Worker %d processed event for user %s", workerID, activity.UserID)
		}
	}
}

package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"analytics/internal/db"

	"github.com/jackc/pgx/v5/pgtype"
)

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
		err = p.queries.UpsertUserMetrics(ctx, db.UpsertUserMetricsParams{
			UserID:        activity.UserID,
			PageViewCount: pageViewIncrement,
			LastActiveAt:  pgtype.Timestamp{Time: activity.Timestamp, Valid: true},
		})

		if err != nil {
			log.Printf("Worker %d failed to update database: %v", workerID, err)
			// Note: In a real system, you might implement exponential backoff here
		} else {
			log.Printf("Worker %d processed event for user %s", workerID, activity.UserID)
		}
	}
}

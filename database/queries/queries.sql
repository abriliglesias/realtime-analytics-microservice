-- file: database/queries/user_metrics.sql

-- name: UpsertUserMetrics :exec
-- Called by the consumer worker for every incoming event.
-- The ON CONFLICT clause performs an atomic read-modify-write inside Postgres,
-- which is safe under the concurrent writes produced by the 3-worker pool.
INSERT INTO user_metrics (user_id, page_view_count, last_active_at)
VALUES ($1, $2, $3)
ON CONFLICT (user_id)
DO UPDATE SET
    page_view_count = user_metrics.page_view_count + EXCLUDED.page_view_count,
    last_active_at  = EXCLUDED.last_active_at;

-- name: GetUserMetrics :one
-- Called by GET /metrics?user_id=X in the producer API.
-- This is an O(1) primary-key lookup — no aggregation required at read time
-- because all counting was done atomically at write time.
SELECT
    user_id,
    page_view_count,
    last_active_at
FROM user_metrics
WHERE user_id = $1;
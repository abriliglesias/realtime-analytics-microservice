-- name: UpsertUserActivity :exec
INSERT INTO user_metrics (user_id, page_view_count, last_active_timestamp)
VALUES ($1, $2, $3)
ON CONFLICT (user_id)
DO UPDATE SET 
    page_view_count = user_metrics.page_view_count + EXCLUDED.page_view_count,
    last_active_timestamp = EXCLUDED.last_active_timestamp;
    
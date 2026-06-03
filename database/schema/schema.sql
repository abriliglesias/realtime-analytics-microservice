CREATE TABLE user_metrics (
    user_id VARCHAR(50) PRIMARY KEY,
    page_view_count INT NOT NULL DEFAULT 0,
    last_active_timestamp TIMESTAMP NOT NULL
);
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

// InitDB sets up the database connection pool and returns a *sql.DB
func InitDB(dbURL string) (*sql.DB, error) {
	// We use the stdlib adapter to make pgx compatible with sql.DB
	// This is important because sqlc generates code expecting *sql.DB
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	// Verify the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	log.Println("Database connection established successfully.")
	return db, nil
}

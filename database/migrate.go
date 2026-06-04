package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	// 1. Connect to the local Docker database
	dbURL := "postgres://user:password@localhost:5432/analytics?sslmode=disable"
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Could not connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Could not ping database. Is it running?: %v", err)
	}

	// 2. Read the schema.sql file
	// (Since we run this from inside the database folder, the path is relative to it)
	sqlBytes, err := os.ReadFile("schema/schema.sql")
	if err != nil {
		log.Fatalf("Could not read schema file: %v", err)
	}

	// 3. Execute the SQL command
	_, err = db.Exec(string(sqlBytes))
	if err != nil {
		log.Fatalf("Failed to execute migration: %v", err)
	}

	log.Println("Database successfully initialized via Go migration!")
}

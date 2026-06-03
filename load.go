package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"sync"
)

func main() {
	// Use localhost by default for local execution.
	// If running inside Docker, override with LOAD_TEST_URL.
	url := os.Getenv("LOAD_TEST_URL")
	if url == "" {
		url = "http://localhost:8080/events"
	}

	// We will simulate 15 fast clicks from the same user
	payload := []byte(`{"user_id": "load_test_user", "activity_type": "page_view", "timestamp": "2024-07-01T12:34:56Z", "metadata": {"page_url": "https://example.com/test"}}`)

	var wg sync.WaitGroup
	numRequests := 15

	fmt.Printf("Firing %d concurrent requests to %s...\n", numRequests, url)

	for i := 1; i <= numRequests; i++ {
		wg.Add(1)
		go func(requestID int) {
			defer wg.Done()
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
			if err != nil {
				fmt.Printf("Request %d failed: %v\n", requestID, err)
				return
			}
			defer resp.Body.Close()
			fmt.Printf("Request %d sent! (Status: %s)\n", requestID, resp.Status)
		}(i)
	}

	wg.Wait()
	fmt.Println("All events fired successfully!")
}

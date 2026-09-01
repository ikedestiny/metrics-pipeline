package tests

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestHighFrequencyIngestionLoad(t *testing.T) {
	// Only run this test when explicitly targeting load benchmarking parameters
	if testing.Short() {
		t.Skip("Skipping high performance load execution metrics")
	}

	targetURL := "http://localhost:8080/api/v1/metrics"
	payload := []byte(`{"service":"payment-gateway","metric":"transaction_duration_ms","value":42.12,"timestamp":1756500000}`)

	totalRequests := 15000 // Exceeds our 10,000 internal ring cap buffer
	concurrencyLimit := 100

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrencyLimit)

	start := time.Now()
	fmt.Printf("Launching %d stress requests at %s with a concurrency floor of %d...\n", totalRequests, targetURL, concurrencyLimit)

	successCount := 0
	rejectedCount := 0
	var mu sync.Mutex

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // Block if concurrency ceiling hit

		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()

			req, _ := http.NewRequest("POST", targetURL, bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				rejectedCount++
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusAccepted {
				successCount++
			} else if resp.StatusCode == http.StatusServiceUnavailable {
				rejectedCount++ // Caught cleanly by our Strategy A backpressure
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n--- Stress Load Report ---\n")
	fmt.Printf("Total Processing Time: %v\n", duration)
	fmt.Printf("Successful Ingestions (202 Accepted): %d\n", successCount)
	fmt.Printf("Backpressure Bounces (503 / Network drops): %d\n", rejectedCount)
	fmt.Printf("Throughput Rate: %.2f requests/sec\n", float64(totalRequests)/duration.Seconds())
}

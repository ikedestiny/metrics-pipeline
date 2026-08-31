package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/ikedestiny/metrics-pipeline/internal/config"
)

func main() {
	// Initialize structured logging (industry standard)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	logger.Info("Starting metrics ingestion server",
		"port", cfg.Port,
		"workers", cfg.WorkerCount,
		"queue_capacity", cfg.QueueCapacity,
	)

	// We will start our HTTP server here in the next step!
	fmt.Println("Bootstrap complete.")
}

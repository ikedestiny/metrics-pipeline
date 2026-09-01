package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/ikedestiny/metrics-pipeline/internal/config"
	"github.com/ikedestiny/metrics-pipeline/internal/domain"
	"github.com/ikedestiny/metrics-pipeline/internal/ingestion"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	cfg := config.Load()

	// 1. Initialize our bounded, thread-safe memory queue (shared infrastructure)
	metricsQueue := make(chan domain.MetricFrame, cfg.QueueCapacity)

	// 2. Pass the queue resource into our HTTP layer
	ingestHandler := ingestion.NewHandler(metricsQueue)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/metrics", ingestHandler.HandleIngest)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	slog.Info("Starting metrics ingestion server",
		"port", cfg.Port,
		"workers", cfg.WorkerCount,
		"queue_capacity", cfg.QueueCapacity,
	)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server failed to start", "error", err.Error())
		os.Exit(1)
	}
}

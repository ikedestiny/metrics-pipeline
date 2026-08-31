package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/ikedestiny/metrics-pipeline/internal/config"
	"github.com/ikedestiny/metrics-pipeline/internal/ingestion"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	cfg := config.Load()

	ingestHandler := ingestion.NewHandler()

	mux := http.NewServeMux()
	// Old syntax - works with Go 1.21 and earlier
	mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ingestHandler.HandleIngest(w, r)
	})

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

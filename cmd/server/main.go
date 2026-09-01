package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ikedestiny/metrics-pipeline/internal/config"
	"github.com/ikedestiny/metrics-pipeline/internal/domain"
	"github.com/ikedestiny/metrics-pipeline/internal/ingestion"
	"github.com/ikedestiny/metrics-pipeline/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	cfg := config.Load()

	// 1. Initialize shared resources
	metricsQueue := make(chan domain.MetricFrame, cfg.QueueCapacity)
	pub := worker.NewConsolePublisher()

	// 2. Set up context lifecycle handling for background workers
	workerCtx, cancelWorkers := context.WithCancel(context.Background())

	// 3. Initialize and boot background worker pool
	pool := worker.NewPool(metricsQueue, pub, cfg.WorkerCount, 500, 100*time.Millisecond)
	pool.Start(workerCtx)

	// 4. Pass queue to HTTP Ingestion Layer
	ingestHandler := ingestion.NewHandler(metricsQueue)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/metrics", ingestHandler.HandleIngest)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	// 5. Intercept OS signals on a separate channel background loop
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Launch HTTP Server inside its own background goroutine
	go func() {
		slog.Info("Starting metrics ingestion server",
			"port", cfg.Port,
			"workers", cfg.WorkerCount,
			"queue_capacity", cfg.QueueCapacity,
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed to start", "error", err.Error())
			os.Exit(1)
		}
	}()

	// Block here until a termination signal lands from the OS
	sig := <-shutdownSignal
	slog.Info("System shutdown signal captured", "signal", sig.String())

	// 6. Execute graceful sequence with a hard 5-second deadline
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	slog.Info("Shutting down HTTP endpoint listener...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server forced to shutdown", "error", err.Error())
	}

	slog.Info("Closing internal metrics queue channel...")
	close(metricsQueue) // Stops ingestion handler from accepting metrics, allows workers to drain it

	slog.Info("Signaling background worker threads to flush residual batches...")
	cancelWorkers()

	// Small pause to guarantee worker logs print before main thread exits
	time.Sleep(200 * time.Millisecond)
	slog.Info("Graceful termination protocol complete. Goodbye.")
}

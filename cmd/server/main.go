package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // Implicitly hooks up runtime profile trees automatically
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

	metricsQueue := make(chan domain.MetricFrame, cfg.QueueCapacity)
	pub := worker.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	workerCtx, cancelWorkers := context.WithCancel(context.Background())

	pool := worker.NewPool(metricsQueue, pub, cfg.WorkerCount, 500, 100*time.Millisecond)
	pool.Start(workerCtx)

	ingestHandler := ingestion.NewHandler(metricsQueue)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/metrics", ingestHandler.HandleIngest)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	// Launch pprof Admin Debug Monitor server on port 6060
	go func() {
		slog.Info("Starting administrative profile server on :6060 (/debug/pprof/)")
		if err := http.ListenAndServe(":6060", nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("pprof listener fell over", "error", err.Error())
		}
	}()

	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		slog.Info("Starting metrics ingestion server",
			"port", cfg.Port,
			"brokers", cfg.KafkaBrokers,
			"topic", cfg.KafkaTopic,
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed to start", "error", err.Error())
			os.Exit(1)
		}
	}()

	sig := <-shutdownSignal
	slog.Info("System shutdown signal captured", "signal", sig.String())

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	slog.Info("Shutting down HTTP endpoint listener...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server forced to shutdown", "error", err.Error())
	}

	slog.Info("Closing internal metrics queue channel...")
	close(metricsQueue)

	slog.Info("Signaling background worker threads to flush residual batches...")
	cancelWorkers()

	slog.Info("Closing Kafka connection infrastructure...")
	if err := pub.Close(); err != nil {
		slog.Error("Failed to cleanly disconnect from Kafka cluster", "error", err.Error())
	}

	time.Sleep(200 * time.Millisecond)
	slog.Info("Graceful termination protocol complete. Goodbye.")
}

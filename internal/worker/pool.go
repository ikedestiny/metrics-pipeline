package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/ikedestiny/metrics-pipeline/internal/domain"
)

type Pool struct {
	queue        chan domain.MetricFrame
	publisher    Publisher
	workerCount  int
	batchSize    int
	batchTimeout time.Duration
}

func NewPool(queue chan domain.MetricFrame, pub Publisher, workers, bSize int, timeout time.Duration) *Pool {
	return &Pool{
		queue:        queue,
		publisher:    pub,
		workerCount:  workers,
		batchSize:    bSize,
		batchTimeout: timeout,
	}
}

// Start spawns the background worker goroutines
func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.workerCount; i++ {
		go p.runWorker(ctx, i)
	}
}

func (p *Pool) runWorker(ctx context.Context, workerID int) {
	slog.Info("Background worker started", "worker_id", workerID)

	batch := make([]domain.MetricFrame, 0, p.batchSize)
	ticker := time.NewTicker(p.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Safely flush residual metrics during shutdown before exiting
			if len(batch) > 0 {
				p.flush(batch, workerID)
			}
			return

		case frame, open := <-p.queue:
			if !open {
				if len(batch) > 0 {
					p.flush(batch, workerID)
				}
				return
			}

			batch = append(batch, frame)

			if len(batch) >= p.batchSize {
				p.flush(batch, workerID)
				batch = make([]domain.MetricFrame, 0, p.batchSize)
				ticker.Reset(p.batchTimeout) // Reset window timer
			}

		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(batch, workerID)
				batch = make([]domain.MetricFrame, 0, p.batchSize)
			}
		}
	}
}

func (p *Pool) flush(batch []domain.MetricFrame, workerID int) {
	slog.Debug("Worker flushing batch", "worker_id", workerID, "count", len(batch))
	if err := p.publisher.PublishBatch(batch); err != nil {
		slog.Error("Failed to publish batch", "worker_id", workerID, "error", err.Error())
	}
}

package worker

import (
	"log/slog"

	"github.com/ikedestiny/metrics-pipeline/internal/domain"
)

type ConsolePublisher struct{}

func NewConsolePublisher() *ConsolePublisher {
	return &ConsolePublisher{}
}

func (c *ConsolePublisher) PublishBatch(batch []domain.MetricFrame) error {
	slog.Info("Successfully published batch to upstream broker", "batch_size", len(batch))
	return nil
}

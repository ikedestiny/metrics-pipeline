package worker

import "github.com/ikedestiny/metrics-pipeline/internal/domain"

// Publisher defines the contract for sending batches of metrics to a broker
type Publisher interface {
	PublishBatch(batch []domain.MetricFrame) error
}

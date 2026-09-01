package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ikedestiny/metrics-pipeline/internal/domain"
)

// MockPublisher tracks items flushed by our worker engine
type MockPublisher struct {
	mu           sync.Mutex
	TotalFlushed int
}

func (m *MockPublisher) PublishBatch(batch []domain.MetricFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalFlushed += len(batch)
	return nil
}

func TestWorkerPoolBatching(t *testing.T) {
	queue := make(chan domain.MetricFrame, 100)
	mockPub := &MockPublisher{}

	// Create a fast timeout pool for unit testing
	pool := NewPool(queue, mockPub, 2, 5, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// Send 7 metrics (Should split into 1 full batch of 5 and 1 timeout batch of 2)
	for i := 0; i < 7; i++ {
		queue <- domain.MetricFrame{Service: "test", Metric: "cpu", Value: 1.0, Timestamp: 123}
	}

	// Give workers enough time to process and trigger the ticker window
	time.Sleep(50 * time.Millisecond)
	cancel()

	mockPub.mu.Lock()
	flushed := mockPub.TotalFlushed
	mockPub.mu.Unlock()

	if flushed != 7 {
		t.Errorf("Expected 7 total items flushed to broker, got %d", flushed)
	}
}

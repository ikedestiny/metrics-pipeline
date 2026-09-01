package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ikedestiny/metrics-pipeline/internal/domain"
)

type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher initializes a cluster-pooled producer configuration
func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{}, // Dynamic balancing across partitions
			BatchSize:    1,                   // Handled by our internal worker-pool queue buffer
			WriteTimeout: 3 * time.Second,
			Async:        false, // Synchronous batch delivery verification
		},
	}
}

// PublishBatch serializes our micro-batches and commits them directly to the partition head
func (k *KafkaPublisher) PublishBatch(batch []domain.MetricFrame) error {
	messages := make([]kafka.Message, len(batch))

	for i, frame := range batch {
		payload, err := json.Marshal(frame)
		if err != nil {
			return fmt.Errorf("failed to marshal frame into kafka payload: %w", err)
		}

		messages[i] = kafka.Message{
			Key:   []byte(frame.Service),
			Value: payload,
		}
	}

	// Ship the full payload array as a single transactional request bundle
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := k.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("kafka write transaction failed: %w", err)
	}

	return nil
}

// Close safely drains remaining connection allocations
func (k *KafkaPublisher) Close() error {
	return k.writer.Close()
}

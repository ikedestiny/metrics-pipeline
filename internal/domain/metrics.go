package domain

import "errors"

// MetricFrame represents the incoming JSON payload
type MetricFrame struct {
	Service   string  `json:"service"`
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

// Validate checks if the incoming metric data is correct
func (m *MetricFrame) Validate() error {
	if m.Service == "" {
		return errors.New("service name cannot be empty")
	}
	if m.Metric == "" {
		return errors.New("metric name cannot be empty")
	}
	if m.Timestamp <= 0 {
		return errors.New("invalid timestamp")
	}
	return nil
}

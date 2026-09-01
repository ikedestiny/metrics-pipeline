package ingestion

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ikedestiny/metrics-pipeline/internal/domain"
)

func TestHandleIngest(t *testing.T) {
	tests := []struct {
		name           string
		payload        string
		expectedStatus int
	}{
		{
			name:           "Valid Payload",
			payload:        `{"service":"payment-service","metric":"latency","value":124.5,"timestamp":1756500000}`,
			expectedStatus: http.StatusAccepted,
		},
		{
			name:           "Empty Service Name",
			payload:        `{"service":"","metric":"latency","value":124.5,"timestamp":1756500000}`,
			expectedStatus: http.StatusUnprocessableEntity,
		},
		{
			name:           "Malformed JSON",
			payload:        `{"service":"payment-service", "metric": }`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	// Provide a queue with enough capacity for the valid test case
	mockQueue := make(chan domain.MetricFrame, 10)
	handler := NewHandler(mockQueue)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("POST", "/api/v1/metrics", bytes.NewBufferString(tt.payload))
			if err != nil {
				t.Fatalf("could not create request: %v", err)
			}

			rec := httptest.NewRecorder()
			handler.HandleIngest(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rec.Code)
			}
		})
	}
}

func TestBackpressureRejection(t *testing.T) {
	// Create a tiny channel that can only hold 1 message
	tinyQueue := make(chan domain.MetricFrame, 1)
	handler := NewHandler(tinyQueue)

	validPayload := `{"service":"payment-service","metric":"latency","value":10.0,"timestamp":1756500000}`

	// 1. Fill the channel to capacity
	req1, _ := http.NewRequest("POST", "/api/v1/metrics", bytes.NewBufferString(validPayload))
	rec1 := httptest.NewRecorder()
	handler.HandleIngest(rec1, req1)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("Expected first message to be accepted, got %d", rec1.Code)
	}

	// 2. This second fast request should trigger Strategy A Backpressure (503)
	req2, _ := http.NewRequest("POST", "/api/v1/metrics", bytes.NewBufferString(validPayload))
	rec2 := httptest.NewRecorder()
	handler.HandleIngest(rec2, req2)

	if rec2.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected backpressure HTTP 503 status code, got %d", rec2.Code)
	}
}

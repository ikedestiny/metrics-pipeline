package ingestion

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
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

	handler := NewHandler()

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

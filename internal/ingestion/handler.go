package ingestion

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/ikedestiny/metrics-pipeline/internal/domain"
)

// Handler handles metrics ingestion HTTP requests
type Handler struct {
	queue chan domain.MetricFrame
}

func NewHandler(queue chan domain.MetricFrame) *Handler {
	return &Handler{
		queue: queue,
	}
}

func (h *Handler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	// Strict JSON parsing
	var frame domain.MetricFrame
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields() // Security best practice

	if err := decoder.Decode(&frame); err != nil {
		slog.Warn("failed to decode metrics payload", "error", err.Error())
		respondWithError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	// Validate the structural data using our domain logic
	if err := frame.Validate(); err != nil {
		slog.Warn("metrics validation failed", "error", err.Error())
		respondWithError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	// Strategy A - Backpressure Check (Non-blocking select)
	select {
	case h.queue <- frame:
		// Message successfully queued into the thread-safe channel buffer!
		slog.Debug("metric queued successfully", "service", frame.Service)
		respondWithJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	default:
		// The 10,000 capacity buffer channel is FULL. Reject the request immediately.
		slog.Error("backpressure triggered: ingestion queue capacity reached")
		respondWithError(w, http.StatusServiceUnavailable, "Ingestion queue full. Try again later.")
	}
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

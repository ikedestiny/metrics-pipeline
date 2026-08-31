package ingestion

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/ikedestiny/metrics-pipeline/internal/domain"
)

// Handler handles metrics ingestion HTTP requests
type Handler struct {
	// We will add our Go channel here in the next step for backpressure!
}

func NewHandler() *Handler {
	return &Handler{}
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

	// Placeholder: In the next step, we will dispatch this into a buffered channel
	slog.Debug("metric validated successfully", "service", frame.Service, "metric", frame.Metric)

	respondWithJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

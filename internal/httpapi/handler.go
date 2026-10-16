// Package httpapi serves the HTTP endpoints of the foresight services.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/te1pe/foresight/internal/scrape"
)

// Pinger is the dependency /readyz checks: the Kafka broker for ingest,
// ClickHouse for the query service. It and the other small interfaces here
// exist so handlers can be tested without live infrastructure.
type Pinger interface {
	Ping(ctx context.Context) error
}

type TargetLister interface {
	Targets() []scrape.TargetStatus
}

const readyTimeout = 2 * time.Second

func NewIngestHandler(dep Pinger, targets TargetLister, pub LogPublisher, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz(log))
	mux.HandleFunc("GET /readyz", readyz(dep, log))
	mux.HandleFunc("GET /v1/targets", listTargets(targets, log))
	mux.HandleFunc("POST /v1/logs", postLogs(pub, log))
	return instrument(mux)
}

// healthz answers "is the process alive" and deliberately ignores
// dependencies: restarting the process would not fix their outage.
func healthz(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"}, log)
	}
}

func readyz(dep Pinger, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()

		if err := dep.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()}, log)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"}, log)
	}
}

// targetJSON is the wire format, kept separate so renaming a field in the
// scraper does not silently change the API.
type targetJSON struct {
	Target string `json:"target"`
	// Up is null until the first scrape: targets start at their own phase
	// within the interval, and "not scraped yet" must not read as "down".
	Up              *bool      `json:"up"`
	LastScrape      *time.Time `json:"last_scrape"`
	DurationSeconds float64    `json:"scrape_duration_seconds"`
	Samples         int        `json:"samples"`
	ParseErrors     int        `json:"parse_errors"`
	LastError       string     `json:"last_error,omitempty"`
	LastErrorAt     *time.Time `json:"last_error_at,omitempty"`
}

func listTargets(targets TargetLister, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statuses := targets.Targets()
		out := make([]targetJSON, 0, len(statuses))
		for _, st := range statuses {
			var up *bool
			if !st.LastScrape.IsZero() {
				up = &st.Up
			}
			out = append(out, targetJSON{
				Target:          st.Target,
				Up:              up,
				LastScrape:      timeOrNil(st.LastScrape),
				DurationSeconds: st.Duration.Seconds(),
				Samples:         st.Samples,
				ParseErrors:     st.ParseErrors,
				LastError:       st.LastError,
				LastErrorAt:     timeOrNil(st.LastErrorAt),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"targets": out}, log)
	}
}

// timeOrNil renders "never happened" as null instead of 0001-01-01.
func timeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func writeJSON(w http.ResponseWriter, status int, v any, log *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Warn("write response", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string, log *slog.Logger) {
	writeJSON(w, status, map[string]string{"error": msg}, log)
}

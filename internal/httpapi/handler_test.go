package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/te1pe/foresight/internal/scrape"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

type fakeTargets []scrape.TargetStatus

func (f fakeTargets) Targets() []scrape.TargetStatus { return f }

func serve(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestHealthAndReadiness(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name   string
		depErr error
		path   string
		method string
		want   int
	}{
		{"healthz ignores the dependency", errors.New("down"), "/healthz", http.MethodGet, http.StatusOK},
		{"readyz with dependency up", nil, "/readyz", http.MethodGet, http.StatusOK},
		{"readyz with dependency down", errors.New("down"), "/readyz", http.MethodGet, http.StatusServiceUnavailable},
		{"wrong method", nil, "/healthz", http.MethodPost, http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewIngestHandler(fakePinger{tt.depErr}, fakeTargets{}, &fakePublisher{}, discard)
			if rec := serve(t, h, tt.method, tt.path); rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestListTargets(t *testing.T) {
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	targets := fakeTargets{
		{Target: "localhost:9100", Up: true, LastScrape: at, Samples: 540, Duration: 20 * time.Millisecond},
		{Target: "localhost:9999", LastScrape: at, Samples: 2, LastError: "connection refused", LastErrorAt: at},
		{Target: "never-scraped"},
	}
	h := NewIngestHandler(fakePinger{}, targets, &fakePublisher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	rec := serve(t, h, http.MethodGet, "/v1/targets")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		Targets []map[string]any `json:"targets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body)
	}
	if len(body.Targets) != 3 {
		t.Fatalf("got %d targets, want 3", len(body.Targets))
	}

	up, down, never := body.Targets[0], body.Targets[1], body.Targets[2]
	if up["up"] != true || up["samples"] != float64(540) || up["scrape_duration_seconds"] != 0.02 {
		t.Errorf("up target = %v", up)
	}
	if _, has := up["last_error"]; has {
		t.Errorf("healthy target has last_error: %v", up)
	}
	if down["up"] != false || down["last_error"] != "connection refused" || down["last_error_at"] == nil {
		t.Errorf("down target = %v", down)
	}
	if never["last_scrape"] != nil || never["up"] != nil {
		t.Errorf("never-scraped target: last_scrape %v, up %v; want both null", never["last_scrape"], never["up"])
	}
}

// The admin package imports net/http/pprof, which registers itself on
// http.DefaultServeMux. The public handlers must not expose it.
func TestPublicHandlersDoNotServePprof(t *testing.T) {
	_ = pprof.Index // make sure the side-effect registration happened
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	handlers := map[string]http.Handler{
		"ingest": NewIngestHandler(fakePinger{}, fakeTargets{}, &fakePublisher{}, discard),
		"query":  NewQueryHandler(&fakeQuerier{}, fakePinger{}, discard),
	}
	for name, h := range handlers {
		if rec := serve(t, h, http.MethodGet, "/debug/pprof/"); rec.Code != http.StatusNotFound {
			t.Errorf("%s serves /debug/pprof/ with status %d", name, rec.Code)
		}
	}
}

func TestRequestMetricsUseRoutePatterns(t *testing.T) {
	h := NewIngestHandler(fakePinger{}, fakeTargets{}, &fakePublisher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	before := testutil.ToFloat64(httpRequests.WithLabelValues("GET /healthz", "200"))
	beforeUnmatched := testutil.ToFloat64(httpRequests.WithLabelValues("unmatched", "404"))

	serve(t, h, http.MethodGet, "/healthz")
	serve(t, h, http.MethodGet, "/wp-admin/setup.php")
	serve(t, h, http.MethodGet, "/random/12345")

	if got := testutil.ToFloat64(httpRequests.WithLabelValues("GET /healthz", "200")) - before; got != 1 {
		t.Errorf("GET /healthz counted %v times, want 1", got)
	}
	if got := testutil.ToFloat64(httpRequests.WithLabelValues("unmatched", "404")) - beforeUnmatched; got != 2 {
		t.Errorf("unknown paths counted %v times under one label, want 2", got)
	}
}

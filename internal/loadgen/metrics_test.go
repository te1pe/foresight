package loadgen

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestQuantile(t *testing.T) {
	// 100 observations: 50 in (0, 0.1], 40 in (0.1, 1], 10 in (1, +Inf).
	b := []bucket{{0.1, 50}, {1, 90}, {math.Inf(1), 100}}
	tests := []struct {
		q, want float64
	}{
		{0.25, 0.05}, // halfway through the first bucket
		{0.5, 0.1},   // exactly its upper bound
		{0.7, 0.55},  // halfway through the second
		{0.95, 1},    // in +Inf: only "more than 1" is known
	}
	for _, tt := range tests {
		if got := quantile(tt.q, b); math.Abs(got-tt.want) > 1e-9 {
			t.Errorf("quantile(%v) = %v, want %v", tt.q, got, tt.want)
		}
	}
	if !math.IsNaN(quantile(0.5, []bucket{{1, 0}, {math.Inf(1), 0}})) {
		t.Error("no observations must give NaN")
	}
}

// End to end against a real client_golang registry: what the services
// expose is what the load test reads.
func TestSnapshotOfRealHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "lat_seconds", Buckets: []float64{0.1, 1, 10},
	}, []string{"kind"})
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "rows_total"})
	reg.MustRegister(h, c)
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	before, err := fetchSnapshot(t.Context(), http.DefaultClient, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	for range 9 {
		h.WithLabelValues("samples").Observe(0.05)
	}
	h.WithLabelValues("samples").Observe(5)
	h.WithLabelValues("logs").Observe(5) // other series must not leak in
	c.Add(42)
	after, err := fetchSnapshot(t.Context(), http.DefaultClient, srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	if got := after.sum("rows_total", nil) - before.sum("rows_total", nil); got != 42 {
		t.Errorf("counter delta = %v", got)
	}
	d := delta(after.buckets("lat_seconds", map[string]string{"kind": "samples"}),
		before.buckets("lat_seconds", map[string]string{"kind": "samples"}))
	if p50 := quantile(0.5, d); p50 <= 0 || p50 > 0.1 {
		t.Errorf("p50 = %v, want within the first bucket", p50)
	}
	if p99 := quantile(0.99, d); p99 <= 1 || p99 > 10 {
		t.Errorf("p99 = %v, want within (1, 10]", p99)
	}
}

package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "http", Name: "requests_total",
		Help: "Requests by route pattern and status code.",
	}, []string{"handler", "code"})
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "foresight", Subsystem: "http", Name: "request_duration_seconds",
		Help:    "Request latency by route pattern.",
		Buckets: prometheus.ExponentialBucketsRange(0.0005, 15, 14),
	}, []string{"handler"})
	logLines = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "logs", Name: "lines_total",
		Help: "Pushed log lines: accepted (durable in the queue) or rejected as malformed.",
	}, []string{"result"})
)

// instrument labels requests with the route pattern the mux matched, e.g.
// "GET /v1/series", never the raw path: a scanner probing random URLs would
// otherwise create a new series per URL. Requests matching no route share
// one label. ServeMux records the pattern on the request it is given, so it
// is readable after the call.
func instrument(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		mux.ServeHTTP(rec, r)

		handler := r.Pattern
		if handler == "" {
			handler = "unmatched"
		}
		httpRequests.WithLabelValues(handler, strconv.Itoa(rec.status)).Inc()
		httpDuration.WithLabelValues(handler).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

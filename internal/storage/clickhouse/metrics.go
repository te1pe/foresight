package clickhouse

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	insertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "foresight", Subsystem: "clickhouse", Name: "insert_duration_seconds",
		Help:    "Time of one INSERT round trip.",
		Buckets: prometheus.ExponentialBucketsRange(0.001, 10, 14),
	}, []string{"table"})
	insertedRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "clickhouse", Name: "inserted_rows_total",
		Help: "Rows sent to ClickHouse, duplicates dropped by deduplication included.",
	}, []string{"table"})
	// pipelineLatency is the end-to-end figure: for samples, from the start
	// of the scrape to the moment its INSERT succeeded; for logs, from the
	// moment ingest received them. Queue time, retries and backlog all show
	// up here, which makes it the number to watch under load.
	pipelineLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "foresight", Name: "pipeline_latency_seconds",
		Help:    "Time from scrape (samples) or receipt (logs) until stored in ClickHouse.",
		Buckets: prometheus.ExponentialBucketsRange(0.01, 600, 18),
	}, []string{"kind"})
)

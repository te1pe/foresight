package queue

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	published = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "queue", Name: "published_total",
		Help: "Records acknowledged by the broker (result=ok) or failed for good (result=error).",
	}, []string{"topic", "result"})
	consumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "queue", Name: "consumed_records_total",
		Help: "Records polled from the broker, before handling.",
	}, []string{"topic"})
	handleDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "foresight", Subsystem: "queue", Name: "handle_duration_seconds",
		Help:    "Time to handle and commit one poll, retries included.",
		Buckets: prometheus.ExponentialBucketsRange(0.001, 60, 14),
	}, []string{"topic"})
	handleRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "queue", Name: "handle_retries_total",
		Help: "Handler failures that were retried; a rising rate means storage is struggling.",
	}, []string{"topic"})
	dropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "queue", Name: "dropped_total",
		Help: "Records skipped for good: undecodable, or rejected permanently by the handler.",
	}, []string{"topic", "reason"})
	commitFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "queue", Name: "commit_failures_total",
		Help: "Offset commits that failed; those records will be redelivered.",
	}, []string{"topic"})
)

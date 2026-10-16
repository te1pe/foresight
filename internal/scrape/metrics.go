package scrape

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Package-level metrics registered on the default registry: the one kind
// of global state the project allows. The target label is bounded by the
// targets table, and series of removed targets are deleted in stop().
var (
	scrapeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "foresight", Subsystem: "scrape", Name: "duration_seconds",
		Help:    "Time to fetch and parse one target, failed scrapes included.",
		Buckets: prometheus.ExponentialBucketsRange(0.005, 10, 12),
	}, []string{"target"})
	scrapeSamples = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "scrape", Name: "samples_total",
		Help: "Samples parsed from targets, synthetic up/scrape_duration_seconds excluded.",
	}, []string{"target"})
	scrapeFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "scrape", Name: "failures_total",
		Help: "Scrapes that recorded up=0.",
	}, []string{"target"})
	scrapeParseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "scrape", Name: "parse_errors_total",
		Help: "Malformed exposition lines skipped.",
	}, []string{"target"})
	scrapeTargets = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "foresight", Subsystem: "scrape", Name: "targets",
		Help: "Targets currently being scraped.",
	})
)

func forgetTargetMetrics(target string) {
	scrapeDuration.DeleteLabelValues(target)
	scrapeSamples.DeleteLabelValues(target)
	scrapeFailures.DeleteLabelValues(target)
	scrapeParseErrors.DeleteLabelValues(target)
}

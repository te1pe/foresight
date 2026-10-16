package rules

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	evalDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "foresight", Subsystem: "rules", Name: "evaluation_duration_seconds",
		Help:    "Time of one evaluation round over all rules.",
		Buckets: prometheus.ExponentialBucketsRange(0.01, 60, 12),
	})
	evalErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "rules", Name: "evaluation_errors_total",
		Help: "Rounds in which at least one rule failed.",
	})
	firing = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "foresight", Subsystem: "rules", Name: "firing",
		Help: "Series for which the rule fired in the latest round.",
	}, []string{"rule"})
)

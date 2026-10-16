package alert

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	openIncidents = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "foresight", Subsystem: "alert", Name: "open_incidents",
		Help: "Incidents open after the latest lifecycle round.",
	})
	transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "alert", Name: "transitions_total",
		Help: "Incidents opened and resolved by this alerter.",
	}, []string{"kind"})
	notifications = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "alert", Name: "notifications_total",
		Help: "Outbox events: sent, failed (retried later) or found already sent.",
	}, []string{"result"})
	roundErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "foresight", Subsystem: "alert", Name: "round_errors_total",
		Help: "Rounds that ended with an error.",
	})
)

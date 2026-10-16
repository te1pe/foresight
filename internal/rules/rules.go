// Package rules applies detectors to stored series once a minute and
// records every outcome.
package rules

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/te1pe/foresight/internal/detect"
	chstore "github.com/te1pe/foresight/internal/storage/clickhouse"
)

type Kind string

const (
	// KindForecast extrapolates a declining series to its limit.
	KindForecast Kind = "forecast"
	// KindAnomaly scores the latest rate of change with EWMA z-score.
	KindAnomaly Kind = "anomaly"
	// KindGrowth flags sustained significant growth: leaks.
	KindGrowth Kind = "growth"
)

type Rule struct {
	Name     string
	Kind     Kind
	Metric   string
	Lookback time.Duration

	// Forecast rules.
	Forecast detect.ForecastConfig
	Limit    float64
	// Horizon separates an alert from a fact: "full in 3 days" is
	// recorded, "full in 3 hours" fires.
	Horizon time.Duration

	// Anomaly rules.
	EWMA detect.EWMAConfig

	// Growth rules.
	Growth detect.GrowthConfig
}

// CRDTJournalMetric is the gauge Cloute is expected to expose: the number of
// operations in a document's CRDT journal (a label per document). The name
// is a contract with Cloute, which foresight does not instrument itself.
const CRDTJournalMetric = "cloute_crdt_journal_size"

// diskRate scores how fast free space changes, in bytes per second. The
// level itself is useless for z-scores: free space trends, and a trend is
// not an anomaly. The floor ignores everything below 1 MiB per minute, the
// background churn of logs and caches.
var diskRate = detect.EWMAConfig{Alpha: 0.1, Threshold: 4, Warmup: 10, MinStd: (1 << 20) / 60.0}

// Default is the rule set: disks (node_exporter) and Go services (Cloute).
//
// The one-hour lookback is a deliberate compromise. It reacts to a fill
// within minutes, but it cannot see daily seasonality; regression over a
// window shorter than a few periods reads part of a cycle as a trend. The
// horizon keeps that harmless while the daily swing is small compared to
// free space (see TestForecastSeasonality).
var Default = []Rule{
	{
		Name:     "disk_fill_forecast",
		Kind:     KindForecast,
		Metric:   "node_filesystem_avail_bytes",
		Lookback: time.Hour,
		Forecast: detect.ForecastConfig{MinPoints: 10, MinTStat: 3, Changepoint: diskRate},
		Limit:    0,
		Horizon:  24 * time.Hour,
	},
	{
		Name:     "disk_usage_rate_anomaly",
		Kind:     KindAnomaly,
		Metric:   "node_filesystem_avail_bytes",
		Lookback: time.Hour,
		EWMA:     diskRate,
	},
	// go_goroutines is exported by every Go program instrumented with the
	// Prometheus client, so this rule covers Cloute and any other Go
	// service as soon as it is a target. A leak shows as steady growth that
	// lasts; 50 extra goroutines and +20% filter out normal churn.
	{
		Name:     "goroutine_leak",
		Kind:     KindGrowth,
		Metric:   "go_goroutines",
		Lookback: time.Hour,
		Growth: detect.GrowthConfig{
			MinPoints: 20, MinTStat: 3, MinSpan: 30 * time.Minute, MinRelative: 0.2, MinAbsolute: 50,
			Changepoint: detect.EWMAConfig{Alpha: 0.1, Threshold: 4, Warmup: 10, MinStd: 0.05},
		},
	},
	// A CRDT journal grows with every edit and shrinks at compaction. Growth
	// that has gone 45 minutes without a compaction (the drop resets the
	// clock) and has at least doubled by half means compaction is not
	// running.
	{
		Name:     "crdt_journal_growth",
		Kind:     KindGrowth,
		Metric:   CRDTJournalMetric,
		Lookback: time.Hour,
		Growth: detect.GrowthConfig{
			MinPoints: 20, MinTStat: 3, MinSpan: 45 * time.Minute, MinRelative: 0.5, MinAbsolute: 1000,
			Changepoint: detect.EWMAConfig{Alpha: 0.1, Threshold: 4, Warmup: 10, MinStd: 1},
		},
	},
}

// Store is what the evaluator needs from storage; an interface so that
// rule logic can be tested on hand-made series.
type Store interface {
	QuerySeries(ctx context.Context, q chstore.SeriesQuery) ([]chstore.Series, error)
	InsertDetections(ctx context.Context, ds []chstore.Detection) error
}

const (
	// evalDelay gives the last closed minute time to arrive: one scrape
	// interval plus queue and insert latency.
	evalDelay = 20 * time.Second
	// maxPoints is far above what an hour of 1m windows for one metric
	// produces; hitting it means a cardinality problem worth an error.
	maxPoints = 200_000
)

type Evaluator struct {
	store Store
	rules []Rule
	log   *slog.Logger
}

func NewEvaluator(store Store, rules []Rule, log *slog.Logger) *Evaluator {
	return &Evaluator{store: store, rules: rules, log: log}
}

// Run evaluates now and then shortly after every minute boundary until ctx
// is cancelled. A failed round is logged and skipped; the next one starts
// from storage again, so nothing is carried over or lost.
func (e *Evaluator) Run(ctx context.Context) {
	for {
		evalTime := time.Now().Truncate(time.Minute)
		if _, err := e.Evaluate(ctx, evalTime); err != nil && ctx.Err() == nil {
			e.log.Error("evaluate rules", "eval_time", evalTime, "err", err)
		}

		timer := time.NewTimer(time.Until(evalTime.Add(time.Minute + evalDelay)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Evaluate runs every rule over the closed 1m windows before evalTime and
// stores the outcome. It is a pure function of stored data and evalTime:
// evaluating the same minute again yields the same rows, which the
// detections table collapses. That is what makes it safe for several
// processors to evaluate concurrently, or for a round to be retried.
func (e *Evaluator) Evaluate(ctx context.Context, evalTime time.Time) ([]chstore.Detection, error) {
	start := time.Now()
	defer func() { evalDuration.Observe(time.Since(start).Seconds()) }()

	var (
		out  []chstore.Detection
		errs []error
	)
	firingByRule := make(map[string]int, len(e.rules))
	for _, r := range e.rules {
		series, err := e.store.QuerySeries(ctx, chstore.SeriesQuery{
			Name: r.Metric, From: evalTime.Add(-r.Lookback), To: evalTime,
			Step: chstore.Step1m, MaxPoints: maxPoints,
		})
		if err != nil {
			// One broken rule must not silence the others.
			errs = append(errs, fmt.Errorf("rule %s: %w", r.Name, err))
			continue
		}
		for _, s := range series {
			d := evaluate(r, s, evalTime)
			if d.Firing {
				firingByRule[r.Name]++
				e.logFiring(d)
			}
			out = append(out, d)
		}
	}

	if err := e.store.InsertDetections(ctx, out); err != nil {
		errs = append(errs, err)
	}
	for _, r := range e.rules {
		firing.WithLabelValues(r.Name).Set(float64(firingByRule[r.Name]))
	}
	if len(errs) > 0 {
		evalErrors.Inc()
	}
	return out, errors.Join(errs...)
}

func evaluate(r Rule, s chstore.Series, evalTime time.Time) chstore.Detection {
	d := chstore.Detection{
		EvalTime: evalTime, Rule: r.Name, Kind: string(r.Kind),
		Target: s.Target, Name: r.Metric, SeriesID: s.ID, Labels: s.Labels,
	}

	// A window's average is attributed to its middle.
	points := make([]detect.Point, len(s.Windows))
	for i, w := range s.Windows {
		points[i] = detect.Point{T: w.Start.Add(30 * time.Second), V: w.Avg}
	}
	if len(points) > 0 {
		d.Observed = points[len(points)-1].V
	}

	switch r.Kind {
	case KindForecast:
		f := detect.ForecastExhaustion(points, evalTime, r.Limit, r.Forecast)
		d.Status = string(f.Status)
		d.Points = len(points)
		if f.Status == detect.StatusNotEnoughData {
			return d
		}
		d.Expected, d.Score, d.Slope, d.Points = f.Current, f.Fit.TStat, f.Fit.Slope, f.Fit.N
		if f.Status == detect.StatusExhausts {
			eta := f.ETA
			d.ETA = &eta
			d.Firing = eta <= r.Horizon
		}

	case KindGrowth:
		g := detect.DetectGrowth(points, evalTime, r.Growth)
		d.Status = string(g.Status)
		d.Points = len(points)
		if g.Status == detect.StatusNotEnoughData && g.Fit.N == 0 {
			return d
		}
		d.Expected, d.Score, d.Slope, d.Points = g.Current, g.Fit.TStat, g.Fit.Slope, g.Fit.N
		d.Firing = g.Status == detect.StatusGrowing

	case KindAnomaly:
		rates := detect.Rates(points)
		d.Points = len(rates)
		if len(rates) == 0 {
			d.Status = string(detect.StatusNotEnoughData)
			return d
		}
		last := detect.ScoreEWMA(rates, r.EWMA)[len(rates)-1]
		d.Observed, d.Expected, d.Score = last.Value, last.Mean, last.Z
		switch {
		case !last.Ready:
			d.Status = "warmup"
		case last.Anomaly:
			d.Status, d.Firing = "anomaly", true
		default:
			d.Status = "normal"
		}
	}
	return d
}

func (e *Evaluator) logFiring(d chstore.Detection) {
	attrs := []any{"rule", d.Rule, "target", d.Target, "labels", d.Labels, "observed", d.Observed, "score", d.Score}
	if d.ETA != nil {
		attrs = append(attrs, "eta", d.ETA.Round(time.Minute).String(), "slope_per_second", d.Slope)
	}
	e.log.Warn("detection firing", attrs...)
}

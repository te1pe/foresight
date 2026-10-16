package detect

import (
	"math"
	"time"
)

type GrowthConfig struct {
	MinPoints   int
	MinTStat    float64
	Changepoint EWMAConfig
	// MinSpan is how long the growth must have lasted without a level
	// shift. A leak grows for hours; a CRDT journal grows between
	// compactions, and each compaction (a sharp drop) restarts the clock,
	// so only growth that outlasts the compaction period qualifies.
	MinSpan time.Duration
	// MinRelative and MinAbsolute bound the size of the increase over the
	// span: 8 → 11 goroutines is +37% and meaningless, 2000 → 2100 is
	// +100 and meaningless too. Both have to be exceeded.
	MinRelative float64
	MinAbsolute float64
}

type Growth struct {
	Status Status
	Fit    LinearFit
	// From is the first point after the last level shift; Start and Current
	// are the fitted values there and at the evaluation time.
	From           time.Time
	Start, Current float64
	// Relative is (Current-Start)/|Start|; +Inf when Start is zero.
	Relative float64
}

// DetectGrowth reports sustained, statistically significant growth: the
// signature of a leak (goroutines, memory, a journal that is never
// compacted). It is the mirror image of ForecastExhaustion and shares its
// guard against level shifts: a step up (a burst of connections that then
// stays flat) is not growth, so only points after the last jump count.
//
// What it cannot tell apart is growth caused by rising load from growth
// caused by a leak: both are a significant upward trend. MinSpan and the
// size thresholds make a daily traffic ramp less likely to qualify, but
// telling them apart for sure needs a load metric to correlate with.
func DetectGrowth(points []Point, now time.Time, cfg GrowthConfig) Growth {
	fit, from, ok := fitAfterShift(points, cfg.Changepoint, cfg.MinPoints)
	if !ok {
		return Growth{Status: StatusNotEnoughData, From: from}
	}

	g := Growth{Status: StatusNoTrend, Fit: fit, From: from, Start: fit.At(from), Current: fit.At(now)}
	increase := g.Current - g.Start
	if g.Start != 0 {
		g.Relative = increase / math.Abs(g.Start)
	} else if increase > 0 {
		g.Relative = math.Inf(1)
	}

	if now.Sub(from) < cfg.MinSpan {
		// Not "no trend": the stretch since the last shift is simply too
		// short to judge yet.
		g.Status = StatusNotEnoughData
		return g
	}
	if fit.Slope > 0 && fit.TStat >= cfg.MinTStat && increase >= cfg.MinAbsolute && g.Relative >= cfg.MinRelative {
		g.Status = StatusGrowing
	}
	return g
}

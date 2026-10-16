// Package detect holds the detection math: an EWMA z-score anomaly detector
// and a least-squares exhaustion forecast. It is pure computation over
// slices, with no I/O, so every property is testable on synthetic data.
package detect

import "math"

type EWMAConfig struct {
	// Alpha is the smoothing factor in (0, 1]: the weight of the newest
	// point. 0.1 gives an effective memory of about 1/Alpha = 10 points.
	Alpha float64
	// Threshold is the |z| at or above which a point is anomalous.
	Threshold float64
	// Warmup is how many points are absorbed before scoring starts; the
	// first variance estimates are built from almost nothing.
	Warmup int
	// MinStd floors the standard deviation. A perfectly flat series has
	// zero variance, and without a floor its first wiggle of one byte
	// would score as infinitely anomalous.
	MinStd float64
}

type ZScore struct {
	Value float64
	// Mean and Std are the expectation before this point was seen: a point
	// is judged against the past, never against itself.
	Mean, Std float64
	Z         float64
	Ready     bool // false during warmup; Z and Anomaly are then meaningless
	Anomaly   bool
}

// ScoreEWMA scores every value against an exponentially weighted mean and
// variance of the values before it.
//
// The estimate is updated with every point, anomalous or not. After a level
// shift the detector therefore flags the shift and then accepts the new
// level as normal within a few points; not updating on anomalies would keep
// flagging the new level forever.
//
// Non-finite values are reported as not ready and do not touch the state.
func ScoreEWMA(values []float64, cfg EWMAConfig) []ZScore {
	out := make([]ZScore, len(values))
	var (
		mean, variance float64
		seen           int
	)
	for i, x := range values {
		out[i].Value = x
		if math.IsNaN(x) || math.IsInf(x, 0) {
			continue
		}
		if seen == 0 {
			mean, seen = x, 1
			continue
		}

		std := max(math.Sqrt(variance), cfg.MinStd)
		diff := x - mean
		z := ZScore{Value: x, Mean: mean, Std: std, Ready: seen >= cfg.Warmup}
		if std > 0 {
			z.Z = diff / std
		}
		z.Anomaly = z.Ready && math.Abs(z.Z) >= cfg.Threshold
		out[i] = z

		// Incremental exponentially weighted mean and variance (West 1979,
		// Finch 2009): O(1) per point, no window of past values kept.
		incr := cfg.Alpha * diff
		mean += incr
		variance = (1 - cfg.Alpha) * (variance + diff*incr)
		seen++
	}
	return out
}

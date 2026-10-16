package detect

import (
	"math"
	"time"
)

type Point struct {
	T time.Time
	V float64
}

// LinearFit is an ordinary least squares line v = Intercept + Slope*(t-Origin).
type LinearFit struct {
	// Origin is the mean time of the points. Centering x there keeps the
	// sums small: with raw Unix seconds (~1.8e9) squared, float64 would lose
	// most of the precision the slope depends on.
	Origin    time.Time
	Intercept float64
	Slope     float64 // units per second
	// TStat is Slope divided by its standard error: how many standard
	// errors the slope is away from zero. It answers "is there a trend or
	// just noise", and unlike R² it accounts for the number of points.
	TStat float64
	R2    float64
	N     int
}

func (f LinearFit) At(t time.Time) float64 {
	return f.Intercept + f.Slope*t.Sub(f.Origin).Seconds()
}

// FitLinear returns false when the line is undefined: fewer than three
// points (no residual degrees of freedom) or all points at one instant.
func FitLinear(points []Point) (LinearFit, bool) {
	n := len(points)
	if n < 3 {
		return LinearFit{}, false
	}

	base := points[0].T
	var sumX, sumY float64
	for _, p := range points {
		sumX += p.T.Sub(base).Seconds()
		sumY += p.V
	}
	meanX, meanY := sumX/float64(n), sumY/float64(n)
	origin := base.Add(time.Duration(meanX * float64(time.Second)))

	var sxx, sxy, syy float64
	for _, p := range points {
		dx := p.T.Sub(base).Seconds() - meanX
		dy := p.V - meanY
		sxx += dx * dx
		sxy += dx * dy
		syy += dy * dy
	}
	if sxx == 0 {
		return LinearFit{}, false
	}

	slope := sxy / sxx
	// Residual sum of squares without a second pass: SSE = Syy - b*Sxy.
	sse := max(syy-slope*sxy, 0)
	fit := LinearFit{Origin: origin, Intercept: meanY, Slope: slope, N: n}
	if syy > 0 {
		fit.R2 = 1 - sse/syy
	}

	stdErr := math.Sqrt(sse / float64(n-2) / sxx)
	switch {
	case stdErr > 0:
		fit.TStat = slope / stdErr
	case slope != 0:
		// A perfect line: the trend is certain.
		fit.TStat = math.Copysign(math.Inf(1), slope)
	}
	return fit, true
}

// Status is the verdict of a trend detector.
type Status string

const (
	// StatusNotEnoughData: too few points, or a level shift happened so
	// recently that too few points follow it.
	StatusNotEnoughData Status = "not_enough_data"
	// StatusNoTrend: the series is flat, growing, or its decline is not
	// distinguishable from noise.
	StatusNoTrend Status = "no_trend"
	// StatusExhausts: a significant decline that reaches the limit.
	StatusExhausts Status = "exhausts"
	// StatusGrowing: sustained, significant growth (see DetectGrowth).
	StatusGrowing Status = "growing"
)

type ForecastConfig struct {
	MinPoints int
	// MinTStat is how many standard errors below zero the slope must be.
	// Around 3, pure noise produces a false trend in well under 1% of fits.
	MinTStat float64
	// Changepoint scores the rate of change between consecutive points;
	// the fit only uses points after the last anomalous jump.
	Changepoint EWMAConfig
}

type Forecast struct {
	Status Status
	Fit    LinearFit
	// From is the first point the fit used: later than the window start
	// when a level shift was detected inside the window.
	From time.Time
	// Current is the fitted value at the evaluation time, less noisy than
	// the last observation.
	Current float64
	// ETA is the time from now until the fitted line reaches the limit;
	// zero when it already has. Set only for StatusExhausts.
	ETA time.Duration
}

// ForecastExhaustion extrapolates a declining series (free bytes, free
// connections) to the moment it reaches limit.
//
// A plain regression over the window misreads a level shift: a disk that
// lost 50 GB in one copy and then stayed flat produces a steep negative
// slope and a false "full in 2 hours". So the rate of change is scored
// with the EWMA detector first, and the fit only uses the points after the
// last jump. A genuine fill that starts mid-window is handled the same way:
// its start is a jump in the rate, and the fit sees only the fill.
func ForecastExhaustion(points []Point, now time.Time, limit float64, cfg ForecastConfig) Forecast {
	fit, from, ok := fitAfterShift(points, cfg.Changepoint, cfg.MinPoints)
	if !ok {
		return Forecast{Status: StatusNotEnoughData, From: from}
	}
	f := Forecast{Status: StatusNoTrend, Fit: fit, From: from, Current: fit.At(now)}
	if fit.Slope >= 0 || fit.TStat > -cfg.MinTStat {
		return f
	}

	f.Status = StatusExhausts
	if f.Current > limit {
		seconds := (f.Current - limit) / -fit.Slope
		f.ETA = time.Duration(seconds * float64(time.Second))
	}
	return f
}

// fitAfterShift fits a line to the finite points after the last jump in the
// rate of change. ok is false when fewer than minPoints (and at least three)
// remain. from is the first point after the jump, set even when ok is
// false: "waiting for data since the shift at 13:20" is worth reporting.
func fitAfterShift(points []Point, changepoint EWMAConfig, minPoints int) (fit LinearFit, from time.Time, ok bool) {
	pts := finitePoints(points)
	if len(pts) >= 2 {
		pts = pts[lastShift(pts, changepoint):]
	}
	if len(pts) > 0 {
		from = pts[0].T
	}
	if len(pts) < max(minPoints, 3) {
		return LinearFit{}, from, false
	}
	fit, ok = FitLinear(pts)
	return fit, from, ok
}

// Rates returns the per-second rate of change between consecutive points:
// len(points)-1 values, rates[i] spanning points[i]..points[i+1]. A rate,
// not a difference, so that a gap of three missing minutes does not look
// like a jump three times as large. Non-increasing time yields NaN, which
// ScoreEWMA skips.
func Rates(points []Point) []float64 {
	if len(points) < 2 {
		return nil
	}
	rates := make([]float64, len(points)-1)
	for i := range rates {
		dt := points[i+1].T.Sub(points[i].T).Seconds()
		if dt <= 0 {
			rates[i] = math.NaN()
			continue
		}
		rates[i] = (points[i+1].V - points[i].V) / dt
	}
	return rates
}

// lastShift returns the index of the first point after the last anomalous
// jump in the rate of change, or 0 if there was none.
func lastShift(pts []Point, cfg EWMAConfig) int {
	scores := ScoreEWMA(Rates(pts), cfg)
	for i := len(scores) - 1; i >= 0; i-- {
		if scores[i].Anomaly {
			// rates[i] spans pts[i]..pts[i+1]: the new regime starts at i+1.
			return i + 1
		}
	}
	return 0
}

func finitePoints(points []Point) []Point {
	out := make([]Point, 0, len(points))
	for _, p := range points {
		if !math.IsNaN(p.V) && !math.IsInf(p.V, 0) {
			out = append(out, p)
		}
	}
	return out
}

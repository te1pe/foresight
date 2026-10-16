package detect

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// Synthetic series are built from four ingredients — trend, noise, step
// and seasonality — so each detector property is checked in isolation and
// in combination, with known ground truth.

var t0 = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

type series func(i int) float64

func trend(start, perPoint float64) series {
	return func(i int) float64 { return start + perPoint*float64(i) }
}

func step(at int, before, after float64) series {
	return func(i int) float64 {
		if i < at {
			return before
		}
		return after
	}
}

func seasonal(amplitude float64, period int) series {
	return func(i int) float64 { return amplitude * math.Sin(2*math.Pi*float64(i)/float64(period)) }
}

func sum(parts ...series) series {
	return func(i int) float64 {
		var v float64
		for _, p := range parts {
			v += p(i)
		}
		return v
	}
}

// noise is seeded, so every run sees the same "random" data.
func noise(sigma float64, seed uint64) series {
	r := rand.New(rand.NewPCG(seed, 0))
	cache := map[int]float64{}
	return func(i int) float64 {
		if v, ok := cache[i]; ok {
			return v
		}
		cache[i] = r.NormFloat64() * sigma
		return cache[i]
	}
}

func values(s series, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = s(i)
	}
	return out
}

// minutely samples one point per minute, like the 1m windows.
func minutely(s series, n int) []Point {
	out := make([]Point, n)
	for i := range out {
		out[i] = Point{T: t0.Add(time.Duration(i) * time.Minute), V: s(i)}
	}
	return out
}

func anomalies(scores []ZScore) []int {
	var idx []int
	for i, z := range scores {
		if z.Anomaly {
			idx = append(idx, i)
		}
	}
	return idx
}

var ewmaCfg = EWMAConfig{Alpha: 0.1, Threshold: 4, Warmup: 10, MinStd: 1e-9}

func TestEWMANoiseFalsePositiveRate(t *testing.T) {
	const n = 20000
	got := anomalies(ScoreEWMA(values(sum(trend(100, 0), noise(1, 1)), n), ewmaCfg))
	t.Logf("pure noise: %d anomalies in %d points (%.3f%%)", len(got), n, 100*float64(len(got))/n)
	// The EWMA variance is itself a noisy estimate (about 2/Alpha effective
	// points), so the tails are heavier than Gaussian; 0.5% is a loose bound
	// around the ~0.1% this produces.
	if rate := float64(len(got)) / n; rate > 0.005 {
		t.Errorf("false positive rate on pure noise = %.4f, want <= 0.005", rate)
	}
}

func TestEWMAStepIsFlaggedThenAccepted(t *testing.T) {
	const at = 200
	scores := ScoreEWMA(values(sum(step(at, 100, 130), noise(1, 2)), 400), ewmaCfg)
	got := anomalies(scores)
	if !slices.Contains(got, at) {
		t.Fatalf("step at %d not flagged; anomalies at %v", at, got)
	}
	for _, i := range got {
		if i < at || i > at+30 {
			t.Errorf("anomaly at %d: outside the step and its settling period", i)
		}
	}
	// Updating on anomalies is what lets the new level become normal.
	if m := scores[399].Mean; math.Abs(m-130) > 3 {
		t.Errorf("mean long after the step = %.1f, want about 130", m)
	}
}

func TestEWMASpike(t *testing.T) {
	base := sum(trend(100, 0), noise(1, 3))
	spiked := func(i int) float64 {
		if i == 300 {
			return base(i) + 20
		}
		return base(i)
	}
	// Compared against the same noise without the spike: the noise alone may
	// produce an occasional false positive, and the spike must add exactly
	// one anomaly — itself — without disturbing the points after it.
	baseline := anomalies(ScoreEWMA(values(base, 500), ewmaCfg))
	want := slices.Sorted(slices.Values(append(slices.Clone(baseline), 300)))
	if got := anomalies(ScoreEWMA(values(spiked, 500), ewmaCfg)); !slices.Equal(got, want) {
		t.Errorf("anomalies = %v, want baseline %v plus the spike at 300", got, baseline)
	}
}

func TestEWMATrendIsNotAnomalous(t *testing.T) {
	// A steady trend makes the EWMA lag by a constant, and the variance
	// absorbs that lag: z settles near 1 instead of growing.
	const n = 2000
	got := anomalies(ScoreEWMA(values(sum(trend(0, 0.5), noise(1, 4)), n), ewmaCfg))
	if rate := float64(len(got)) / n; rate > 0.005 {
		t.Errorf("anomaly rate on a trend = %.4f (%d points), want <= 0.005", rate, len(got))
	}
}

func TestEWMASeasonality(t *testing.T) {
	const n, period = 2000, 200
	smooth := sum(trend(100, 0), seasonal(10, period), noise(1, 5))
	if got := anomalies(ScoreEWMA(values(smooth, n), ewmaCfg)); float64(len(got))/n > 0.005 {
		t.Errorf("slow seasonality flagged %d times, want <= 0.5%%", len(got))
	}

	// A spike on top of the seasonal swing is still caught.
	spiked := func(i int) float64 {
		if i == 1050 {
			return smooth(i) + 25
		}
		return smooth(i)
	}
	if got := anomalies(ScoreEWMA(values(spiked, n), ewmaCfg)); !slices.Contains(got, 1050) {
		t.Errorf("spike on a seasonal series not flagged; anomalies at %v", got)
	}
}

func TestEWMAEdgeCases(t *testing.T) {
	t.Run("warmup", func(t *testing.T) {
		v := []float64{1, 1, 1, 1000, 1, 1}
		for i, z := range ScoreEWMA(v, ewmaCfg) {
			if z.Ready || z.Anomaly {
				t.Errorf("point %d scored during warmup: %+v", i, z)
			}
		}
	})
	t.Run("flat series and MinStd", func(t *testing.T) {
		v := values(trend(5, 0), 50)
		v[40] += 0.5
		v[45] += 50
		cfg := ewmaCfg
		cfg.MinStd = 1
		if got := anomalies(ScoreEWMA(v, cfg)); !slices.Equal(got, []int{45}) {
			t.Errorf("anomalies = %v, want only the change larger than MinStd*Threshold", got)
		}
	})
	t.Run("non-finite values are skipped", func(t *testing.T) {
		v := values(sum(trend(10, 0), noise(1, 6)), 30)
		v[20] = math.NaN()
		v[21] = math.Inf(1)
		scores := ScoreEWMA(v, ewmaCfg)
		if scores[20].Ready || scores[21].Ready || scores[20].Anomaly || scores[21].Anomaly {
			t.Errorf("non-finite values were scored: %+v %+v", scores[20], scores[21])
		}
		if !scores[22].Ready || math.IsNaN(scores[22].Mean) || scores[22].Anomaly {
			t.Errorf("state was corrupted by non-finite values: %+v", scores[22])
		}
	})
}

func TestFitLinear(t *testing.T) {
	// 1000 - 2/s, sampled every minute.
	pts := minutely(trend(1000, -120), 10)
	fit, ok := FitLinear(pts)
	if !ok {
		t.Fatal("FitLinear failed")
	}
	if math.Abs(fit.Slope+2) > 1e-9 || !math.IsInf(fit.TStat, -1) || math.Abs(fit.R2-1) > 1e-12 {
		t.Errorf("fit = %+v, want slope -2/s, certain trend, R² 1", fit)
	}
	if v := fit.At(t0.Add(time.Minute)); math.Abs(v-880) > 1e-6 {
		t.Errorf("At(+1m) = %v, want 880", v)
	}

	if _, ok := FitLinear(pts[:2]); ok {
		t.Error("fit with two points has no residual degrees of freedom")
	}
	same := []Point{{t0, 1}, {t0, 2}, {t0, 3}}
	if _, ok := FitLinear(same); ok {
		t.Error("fit with all points at one instant must fail")
	}
}

// Disk-like defaults: rates in bytes per second, the floor ignores
// fluctuations below 1 MiB per minute.
var forecastCfg = ForecastConfig{
	MinPoints:   10,
	MinTStat:    3,
	Changepoint: EWMAConfig{Alpha: 0.1, Threshold: 4, Warmup: 5, MinStd: (1 << 20) / 60.0},
}

const gib = 1 << 30

// lastPlus returns the evaluation time: one minute after the last point.
func lastPlus(pts []Point) time.Time { return pts[len(pts)-1].T.Add(time.Minute) }

func TestForecastPureTrend(t *testing.T) {
	// 100 GiB free, losing 1 GiB per minute: at evaluation time (one minute
	// after the 60th point) 40 GiB remain, gone in 40 minutes.
	pts := minutely(trend(100*gib, -gib), 60)
	f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
	if f.Status != StatusExhausts {
		t.Fatalf("status = %s, want exhausts", f.Status)
	}
	if d := f.ETA - 40*time.Minute; d.Abs() > time.Second {
		t.Errorf("ETA = %v, want 40m", f.ETA)
	}
}

func TestForecastTrendWithNoise(t *testing.T) {
	for seed := range uint64(20) {
		pts := minutely(sum(trend(100*gib, -gib), noise(2*gib, seed)), 60)
		f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
		if f.Status != StatusExhausts {
			t.Errorf("seed %d: status %s, want exhausts", seed, f.Status)
			continue
		}
		if rel := math.Abs(f.ETA.Minutes()-40) / 40; rel > 0.15 {
			t.Errorf("seed %d: ETA %v, want 40m ±15%%", seed, f.ETA)
		}
	}
}

func TestForecastNoiseOnlyRarelyClaimsATrend(t *testing.T) {
	falseTrends := 0
	const runs = 300
	for seed := range uint64(runs) {
		pts := minutely(sum(trend(100*gib, 0), noise(gib, seed+1000)), 60)
		if ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg).Status == StatusExhausts {
			falseTrends++
		}
	}
	// Half of |t| >= 3 tail for ~58 degrees of freedom is about 0.2%.
	t.Logf("noise only: %d false trends in %d runs", falseTrends, runs)
	if falseTrends > runs/100 {
		t.Errorf("noise produced %d false trends in %d runs, want <= 1%%", falseTrends, runs)
	}
}

func TestForecastRisingSeriesNeverExhausts(t *testing.T) {
	pts := minutely(sum(trend(10*gib, gib), noise(gib/10, 7)), 60)
	if f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg); f.Status != StatusNoTrend {
		t.Errorf("status = %s, want no_trend", f.Status)
	}
}

func TestForecastStepIsNotATrend(t *testing.T) {
	// One big copy, then nothing: 50 GiB gone at minute 30, flat after.
	pts := minutely(sum(step(30, 150*gib, 100*gib), noise(gib/20, 8)), 60)

	f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
	if f.Status == StatusExhausts {
		t.Errorf("step read as a trend: ETA %v", f.ETA)
	}
	if !f.From.Equal(pts[30].T) {
		t.Errorf("fit starts at %v, want right after the step at %v", f.From, pts[30].T)
	}

	// The same data without changepoint detection: plain regression sees
	// a steep decline. This is exactly the false alarm the changepoint
	// step exists to prevent.
	naive := forecastCfg
	naive.Changepoint.Threshold = math.Inf(1)
	if f := ForecastExhaustion(pts, lastPlus(pts), 0, naive); f.Status != StatusExhausts {
		t.Errorf("control: plain regression should have claimed exhaustion, got %s", f.Status)
	}
}

func TestForecastFillStartingMidWindow(t *testing.T) {
	// Flat for 30 minutes, then losing 1 GiB per minute: the forecast must
	// follow the fill, not average it with the quiet half.
	flat := 100 * gib
	s := func(i int) float64 {
		if i < 30 {
			return float64(flat)
		}
		return float64(flat) - float64(i-29)*gib
	}
	pts := minutely(sum(s, noise(gib/50, 9)), 60)
	f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
	if f.Status != StatusExhausts {
		t.Fatalf("status = %s, want exhausts", f.Status)
	}
	// At evaluation time (minute 60) 100-31 = 69 GiB remain.
	if rel := math.Abs(f.ETA.Minutes()-69) / 69; rel > 0.05 {
		t.Errorf("ETA = %v, want about 69m", f.ETA)
	}
	if f.From.Before(pts[29].T) || f.From.After(pts[31].T) {
		t.Errorf("fit starts at %v, want at the start of the fill (%v)", f.From, pts[30].T)
	}
}

func TestForecastSeasonality(t *testing.T) {
	const period = 60
	// Limitation, pinned down on purpose: regression over a window of a few
	// periods sees a spurious slope (the sine correlates with time), and the
	// t-test calls it significant because seasonal residuals are not
	// independent. What keeps this harmless is magnitude: the false slope is
	// a fraction of amplitude/period, so the ETA lands far beyond any
	// alerting horizon as long as the swing is small relative to the level.
	daily := sum(trend(200*gib, 0), seasonal(2*gib, period), noise(gib/20, 10))
	pts := minutely(daily, 2*period)
	f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
	t.Logf("seasonal without trend: status %s, t=%.1f, ETA %.0f days", f.Status, f.Fit.TStat, f.ETA.Hours()/24)
	if f.Status == StatusExhausts && f.ETA < 7*24*time.Hour {
		t.Errorf("seasonal swing without trend forecast as exhaustion in %v", f.ETA)
	}

	// A real trend under the same seasonality is still estimated: with the
	// window covering whole periods the bias stays within 15%.
	withTrend := sum(trend(100*gib, -gib/2), seasonal(2*gib, period), noise(gib/20, 11))
	pts = minutely(withTrend, 2*period)
	f = ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
	truth := (100.0 - 0.5*float64(2*period)) / 0.5 // minutes left at evaluation time
	t.Logf("trend under seasonality: ETA %.1fm, truth %.0fm", f.ETA.Minutes(), truth)
	if f.Status != StatusExhausts || math.Abs(f.ETA.Minutes()-truth)/truth > 0.15 {
		t.Errorf("trend under seasonality: status %s ETA %v, want about %.0fm", f.Status, f.ETA, truth)
	}
}

func TestForecastEdgeCases(t *testing.T) {
	t.Run("too few points", func(t *testing.T) {
		pts := minutely(trend(100, -1), 5)
		if f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg); f.Status != StatusNotEnoughData {
			t.Errorf("status = %s", f.Status)
		}
	})
	t.Run("already below the limit", func(t *testing.T) {
		pts := minutely(trend(20*gib, -gib), 30)
		f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
		if f.Status != StatusExhausts || f.ETA != 0 {
			t.Errorf("status %s ETA %v, want exhausts with ETA 0", f.Status, f.ETA)
		}
	})
	t.Run("NaN points are ignored", func(t *testing.T) {
		pts := minutely(trend(100*gib, -gib), 60)
		pts[10].V, pts[40].V = math.NaN(), math.Inf(-1)
		f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg)
		if f.Status != StatusExhausts || (f.ETA-40*time.Minute).Abs() > time.Second {
			t.Errorf("status %s ETA %v, want 40m", f.Status, f.ETA)
		}
	})
	t.Run("constant zero is no trend", func(t *testing.T) {
		// autofs mounts report 0 bytes free forever: full, but not filling.
		pts := minutely(trend(0, 0), 60)
		if f := ForecastExhaustion(pts, lastPlus(pts), 0, forecastCfg); f.Status != StatusNoTrend {
			t.Errorf("status = %s, want no_trend", f.Status)
		}
	})
}

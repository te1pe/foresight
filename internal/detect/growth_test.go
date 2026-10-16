package detect

import (
	"math"
	"testing"
	"time"
)

// Goroutine-like defaults: rates in goroutines per second, the floor ignores
// churn below 3 per minute.
var leakCfg = GrowthConfig{
	MinPoints:   20,
	MinTStat:    3,
	Changepoint: EWMAConfig{Alpha: 0.1, Threshold: 4, Warmup: 10, MinStd: 0.05},
	MinSpan:     30 * time.Minute,
	MinRelative: 0.2,
	MinAbsolute: 50,
}

func TestGrowthLeak(t *testing.T) {
	// 200 goroutines, 3 more every minute, never released.
	pts := minutely(sum(trend(200, 3), noise(5, 20)), 60)
	g := DetectGrowth(pts, lastPlus(pts), leakCfg)
	if g.Status != StatusGrowing {
		t.Fatalf("status = %s (t=%.1f, +%.0f%%), want growing", g.Status, g.Fit.TStat, 100*g.Relative)
	}
	if perMinute := g.Fit.Slope * 60; math.Abs(perMinute-3) > 0.3 {
		t.Errorf("slope = %.2f per minute, want about 3", perMinute)
	}
}

func TestGrowthStableServiceRarelyFlagged(t *testing.T) {
	flagged := 0
	const runs = 200
	for seed := range uint64(runs) {
		pts := minutely(sum(trend(200, 0), noise(5, seed+100)), 60)
		if DetectGrowth(pts, lastPlus(pts), leakCfg).Status == StatusGrowing {
			flagged++
		}
	}
	t.Logf("stable service: %d of %d runs flagged", flagged, runs)
	if flagged > runs/100 {
		t.Errorf("stable service flagged %d times in %d runs, want <= 1%%", flagged, runs)
	}
}

func TestGrowthStepUpIsNotALeak(t *testing.T) {
	// A burst of long-lived connections: +200 goroutines at once, then flat.
	pts := minutely(sum(step(30, 200, 400), noise(5, 21)), 60)
	if g := DetectGrowth(pts, lastPlus(pts), leakCfg); g.Status == StatusGrowing {
		t.Errorf("step read as a leak: from %v, t=%.1f", g.From, g.Fit.TStat)
	}

	// Control: without the changepoint the same data looks like a leak.
	naive := leakCfg
	naive.Changepoint.Threshold = math.Inf(1)
	if g := DetectGrowth(pts, lastPlus(pts), naive); g.Status != StatusGrowing {
		t.Errorf("control: plain regression should have flagged the step, got %s", g.Status)
	}
}

func TestGrowthTooSmallToMatter(t *testing.T) {
	// 8 → 14 goroutines: +75%, significant, and irrelevant.
	pts := minutely(sum(trend(8, 0.1), noise(0.2, 22)), 60)
	if g := DetectGrowth(pts, lastPlus(pts), leakCfg); g.Status == StatusGrowing {
		t.Errorf("growth of %.0f goroutines flagged", g.Current-g.Start)
	}
}

// Journal-like defaults: compaction every ~20 minutes is normal, growth
// that has gone 45 minutes without one is not.
var journalCfg = GrowthConfig{
	MinPoints:   20,
	MinTStat:    3,
	Changepoint: EWMAConfig{Alpha: 0.1, Threshold: 4, Warmup: 10, MinStd: 1},
	MinSpan:     45 * time.Minute,
	MinRelative: 0.5,
	MinAbsolute: 1000,
}

func TestGrowthJournalWithCompaction(t *testing.T) {
	// Sawtooth: +50 ops per minute, compacted back to ~0 every 20 minutes.
	sawtooth := func(i int) float64 { return 50 * float64(i%20) }
	pts := minutely(sum(sawtooth, noise(5, 23)), 90)
	g := DetectGrowth(pts, lastPlus(pts), journalCfg)
	if g.Status != StatusNotEnoughData {
		t.Errorf("status = %s, want not_enough_data: 10 minutes since the last compaction", g.Status)
	}
	if !g.From.Equal(pts[80].T) {
		t.Errorf("fit starts at %v, want right after the last compaction %v", g.From, pts[80].T)
	}
}

func TestGrowthJournalWithoutCompaction(t *testing.T) {
	pts := minutely(sum(trend(500, 50), noise(5, 24)), 60)
	if g := DetectGrowth(pts, lastPlus(pts), journalCfg); g.Status != StatusGrowing {
		t.Errorf("status = %s, want growing", g.Status)
	}
}

func TestGrowthSmallDailySwing(t *testing.T) {
	// The rising half of a slow cycle is a real increase within the hour.
	// It stays below the thresholds while the swing is small relative to
	// the level (here ±5%); a larger swing would be flagged — see the
	// DetectGrowth doc on load versus leaks.
	pts := minutely(sum(trend(1000, 0), seasonal(50, 240), noise(3, 25)), 60)
	if g := DetectGrowth(pts, lastPlus(pts), leakCfg); g.Status == StatusGrowing {
		t.Errorf("small daily swing flagged: +%.0f%%", 100*g.Relative)
	}
}

func TestGrowthEdgeCases(t *testing.T) {
	short := minutely(trend(100, 10), 10)
	if g := DetectGrowth(short, lastPlus(short), leakCfg); g.Status != StatusNotEnoughData {
		t.Errorf("10 points: status %s", g.Status)
	}
	// From zero any increase is infinitely large relatively; the absolute
	// threshold still applies.
	fromZero := minutely(trend(0, 5), 60)
	if g := DetectGrowth(fromZero, lastPlus(fromZero), leakCfg); g.Status != StatusGrowing || !math.IsInf(g.Relative, 1) && g.Relative < 1 {
		t.Errorf("growth from zero: status %s relative %v", g.Status, g.Relative)
	}
}

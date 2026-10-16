package clickhouse

import (
	"math"
	"testing"
	"time"

	"github.com/te1pe/foresight/internal/scrape"
)

func TestDetectionsRoundTripAndLatest(t *testing.T) {
	s := newTestStore(t)
	tgt := target(t)
	eta := 3*time.Hour + 20*time.Minute
	first := []Detection{
		{EvalTime: base, Rule: "fill", Kind: "forecast", Status: "exhausts", Firing: true, Target: tgt, Name: "m",
			SeriesID: 1, Labels: map[string]string{"mountpoint": "/"}, Observed: 10, Expected: 9, Score: math.Inf(-1),
			Slope: -0.5, ETA: &eta, Points: 60},
		{EvalTime: base, Rule: "rate", Kind: "anomaly", Status: "normal", Target: tgt, Name: "m", SeriesID: 1, Points: 59},
	}
	if err := s.InsertDetections(t.Context(), first); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// The next minute supersedes the first for "fill"; re-evaluating the
	// same minute (another processor, a retry) must not add a row.
	next := first[0]
	next.EvalTime, next.Firing, next.Status, next.ETA = base.Add(time.Minute), false, "no_trend", nil
	if err := s.InsertDetections(t.Context(), []Detection{next, next}); err != nil {
		t.Fatalf("insert next: %v", err)
	}

	got, err := s.LatestDetections(t.Context(), tgt, base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("LatestDetections: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d detections, want the latest per rule and series: %+v", len(got), got)
	}
	fill, rate := got[0], got[1]
	if fill.Rule != "fill" || !fill.EvalTime.Equal(base.Add(time.Minute)) || fill.Status != "no_trend" || fill.ETA != nil {
		t.Errorf("latest fill = %+v, want the second evaluation", fill)
	}
	if rate.Rule != "rate" || rate.Points != 59 || rate.Labels == nil {
		t.Errorf("rate = %+v", rate)
	}

	var rows uint64
	if err := s.conn.QueryRow(t.Context(), "SELECT count() FROM detections FINAL WHERE target = ?", tgt).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 3 {
		t.Errorf("rows = %d, want 3: the repeated minute must collapse", rows)
	}
}

func TestDetectionETARoundTrip(t *testing.T) {
	s := newTestStore(t)
	tgt := target(t)
	eta := 3*time.Hour + 20*time.Minute
	err := s.InsertDetections(t.Context(), []Detection{{
		EvalTime: base, Rule: "fill", Kind: "forecast", Status: "exhausts", Firing: true, Target: tgt,
		Name: "m", SeriesID: 1, Score: math.Inf(-1), ETA: &eta,
	}})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := s.LatestDetections(t.Context(), tgt, base)
	if err != nil || len(got) != 1 {
		t.Fatalf("LatestDetections: %v, %d rows", err, len(got))
	}
	if got[0].ETA == nil || *got[0].ETA != eta || !got[0].Firing || !math.IsInf(got[0].Score, -1) {
		t.Errorf("got %+v, want ETA %v, firing, score -Inf", got[0], eta)
	}
	if later, _ := s.LatestDetections(t.Context(), tgt, base.Add(time.Minute)); len(later) != 0 {
		t.Errorf("since filter ignored: %+v", later)
	}
}

func TestQueryAcrossTargets(t *testing.T) {
	s := newTestStore(t)
	// A metric name unique to this test stands in for "all targets".
	name := "cross_target_" + t.Name()
	var batches []scrape.Batch
	for _, tgt := range []string{t.Name() + "-b", t.Name() + "-a"} {
		batches = append(batches, scrape.Batch{Target: tgt, Time: base.Add(5 * time.Second), Samples: []scrape.Sample{
			{Name: name, Value: 1, Type: scrape.TypeGauge},
		}})
	}
	if _, err := s.InsertBatches(t.Context(), batches); err != nil {
		t.Fatalf("insert: %v", err)
	}

	for _, step := range []Step{StepRaw, Step1m} {
		got, err := s.QuerySeries(t.Context(), SeriesQuery{Name: name, From: base, To: base.Add(time.Hour), Step: step, MaxPoints: 100})
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if len(got) != 2 || got[0].Target != t.Name()+"-a" || got[1].Target != t.Name()+"-b" {
			t.Errorf("%s: got %+v, want both targets, ordered", step, got)
		}
	}
}

func TestRecentDetectionsHistory(t *testing.T) {
	s := newTestStore(t)
	tgt := target(t)
	var ds []Detection
	for i := range 3 {
		for _, id := range []uint64{2, 1} {
			ds = append(ds, Detection{EvalTime: base.Add(time.Duration(i) * time.Minute), Rule: "r", Kind: "anomaly",
				Status: "normal", Firing: i == 2, Target: tgt, Name: "m", SeriesID: id})
		}
	}
	if err := s.InsertDetections(t.Context(), ds); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := s.RecentDetections(t.Context(), base.Add(time.Minute))
	if err != nil {
		t.Fatalf("RecentDetections: %v", err)
	}
	var mine []Detection
	for _, d := range got {
		if d.Target == tgt {
			mine = append(mine, d)
		}
	}
	// Grouped by series, oldest first, and the "since" bound applied.
	want := []struct {
		id     uint64
		minute int
		firing bool
	}{{1, 1, false}, {1, 2, true}, {2, 1, false}, {2, 2, true}}
	if len(mine) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(mine), len(want), mine)
	}
	for i, w := range want {
		d := mine[i]
		if d.SeriesID != w.id || !d.EvalTime.Equal(base.Add(time.Duration(w.minute)*time.Minute)) || d.Firing != w.firing {
			t.Errorf("row %d = series %d at %v firing %v, want series %d minute %d firing %v",
				i, d.SeriesID, d.EvalTime, d.Firing, w.id, w.minute, w.firing)
		}
	}
}

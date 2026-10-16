package rules

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/te1pe/foresight/internal/detect"
	chstore "github.com/te1pe/foresight/internal/storage/clickhouse"
)

type fakeStore struct {
	series   map[string][]chstore.Series // by metric
	queryErr map[string]error
	queries  []chstore.SeriesQuery
	inserted []chstore.Detection
}

func (f *fakeStore) QuerySeries(_ context.Context, q chstore.SeriesQuery) ([]chstore.Series, error) {
	f.queries = append(f.queries, q)
	if err := f.queryErr[q.Name]; err != nil {
		return nil, err
	}
	return f.series[q.Name], nil
}

func (f *fakeStore) InsertDetections(_ context.Context, ds []chstore.Detection) error {
	f.inserted = append(f.inserted, ds...)
	return nil
}

var evalTime = time.Date(2026, 10, 15, 13, 0, 0, 0, time.UTC)

const gib = 1 << 30

// windows builds 60 one-minute windows ending right before evalTime.
func windows(value func(i int) float64) []chstore.Window {
	ws := make([]chstore.Window, 60)
	for i := range ws {
		ws[i] = chstore.Window{Start: evalTime.Add(time.Duration(i-60) * time.Minute), Avg: value(i)}
	}
	return ws
}

func disk(id uint64, mountpoint string, value func(i int) float64) chstore.Series {
	return chstore.Series{
		Target: "localhost:9100", ID: id, Labels: map[string]string{"mountpoint": mountpoint},
		Windows: windows(value),
	}
}

func newEvaluator(store Store) *Evaluator {
	return NewEvaluator(store, Default, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func find(t *testing.T, ds []chstore.Detection, rule string, id uint64) chstore.Detection {
	t.Helper()
	for _, d := range ds {
		if d.Rule == rule && d.SeriesID == id {
			return d
		}
	}
	t.Fatalf("no detection for rule %s series %d", rule, id)
	return chstore.Detection{}
}

func TestEvaluateDiskRules(t *testing.T) {
	// Tiny wobble so the regression has a finite t-statistic.
	wobble := func(i int) float64 { return float64(i%3) * (1 << 20) }

	store := &fakeStore{series: map[string][]chstore.Series{
		"node_filesystem_avail_bytes": {
			// Losing 1 GiB per minute with 100 GiB left at the start of the
			// window: full in about 40 minutes, inside the 24h horizon.
			disk(1, "/data", func(i int) float64 { return 100*gib - float64(i)*gib + wobble(i) }),
			// Losing 1 GiB per hour with 500 GiB left: a fact, not an alert.
			disk(2, "/", func(i int) float64 { return 500*gib - float64(i)*gib/60 + wobble(i) }),
			// autofs: always 0 free, never changing.
			disk(3, "/home", func(int) float64 { return 0 }),
			// Steady, then 20 GiB vanish in the last minute.
			disk(4, "/tmp", func(i int) float64 {
				if i == 59 {
					return 30*gib + wobble(i)
				}
				return 50*gib + wobble(i)
			}),
		},
	}}

	got, err := newEvaluator(store).Evaluate(t.Context(), evalTime)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(got) != 8 || len(store.inserted) != 8 {
		t.Fatalf("got %d detections, inserted %d; want 2 rules x 4 series", len(got), len(store.inserted))
	}

	q := store.queries[0]
	if q.Target != "" || q.Step != chstore.Step1m || !q.To.Equal(evalTime) || !q.From.Equal(evalTime.Add(-time.Hour)) {
		t.Errorf("query = %+v, want all targets, 1m windows, the hour before evalTime", q)
	}

	fast := find(t, got, "disk_fill_forecast", 1)
	if fast.Status != string(detect.StatusExhausts) || !fast.Firing || fast.ETA == nil {
		t.Fatalf("fast fill: %+v, want firing forecast", fast)
	}
	if d := *fast.ETA - 40*time.Minute; d.Abs() > 2*time.Minute {
		t.Errorf("fast fill ETA = %v, want about 40m", *fast.ETA)
	}
	if fast.Target != "localhost:9100" || fast.Labels["mountpoint"] != "/data" || fast.Slope >= 0 {
		t.Errorf("fast fill metadata: %+v", fast)
	}

	slow := find(t, got, "disk_fill_forecast", 2)
	if slow.Status != string(detect.StatusExhausts) || slow.Firing || slow.ETA == nil || *slow.ETA < 24*time.Hour {
		t.Errorf("slow fill: %+v, want an exhaustion forecast beyond the horizon, not firing", slow)
	}

	if full := find(t, got, "disk_fill_forecast", 3); full.Status != string(detect.StatusNoTrend) || full.Firing {
		t.Errorf("constant zero: %+v, want no_trend", full)
	}

	burst := find(t, got, "disk_usage_rate_anomaly", 4)
	if burst.Status != "anomaly" || !burst.Firing || burst.Score > -4 {
		t.Errorf("burst: %+v, want a firing anomaly with a strongly negative z", burst)
	}
	if steady := find(t, got, "disk_usage_rate_anomaly", 1); steady.Firing || steady.Status != "normal" {
		t.Errorf("steady fill rate: %+v, want normal", steady)
	}
	// Right after a level shift the forecast waits for data instead of
	// extrapolating the jump.
	if f := find(t, got, "disk_fill_forecast", 4); f.Status != string(detect.StatusNotEnoughData) {
		t.Errorf("forecast right after a burst: %+v, want not_enough_data", f)
	}
}

func TestEvaluateKeepsGoingWhenOneRuleFails(t *testing.T) {
	store := &fakeStore{
		series:   map[string][]chstore.Series{"ok_metric": {disk(1, "/", func(int) float64 { return gib })}},
		queryErr: map[string]error{"broken_metric": errors.New("clickhouse down")},
	}
	rules := []Rule{
		{Name: "broken", Kind: KindAnomaly, Metric: "broken_metric", Lookback: time.Hour, EWMA: diskRate},
		{Name: "ok", Kind: KindAnomaly, Metric: "ok_metric", Lookback: time.Hour, EWMA: diskRate},
	}
	e := NewEvaluator(store, rules, slog.New(slog.NewTextHandler(io.Discard, nil)))

	got, err := e.Evaluate(t.Context(), evalTime)
	if err == nil || !strings.Contains(err.Error(), "rule broken") {
		t.Errorf("err = %v, want the broken rule reported", err)
	}
	if len(got) != 1 || got[0].Rule != "ok" || len(store.inserted) != 1 {
		t.Errorf("got %+v, want the healthy rule evaluated and stored", got)
	}
}

func TestEvaluateWithoutData(t *testing.T) {
	store := &fakeStore{series: map[string][]chstore.Series{
		"node_filesystem_avail_bytes": {{Target: "t", ID: 1}},
	}}
	got, err := newEvaluator(store).Evaluate(t.Context(), evalTime)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	for _, d := range got {
		if d.Status != string(detect.StatusNotEnoughData) || d.Firing {
			t.Errorf("%s on an empty series: %+v", d.Rule, d)
		}
	}
}

func series(target string, id uint64, value func(i int) float64) chstore.Series {
	return chstore.Series{Target: target, ID: id, Windows: windows(value)}
}

func TestEvaluateGrowthRules(t *testing.T) {
	jitter := func(i int) float64 { return float64(i%3) - 1 }
	store := &fakeStore{series: map[string][]chstore.Series{
		"go_goroutines": {
			// A leak: 3 goroutines a minute, never released.
			series("cloute:8080", 1, func(i int) float64 { return 200 + 3*float64(i) + jitter(i) }),
			// Healthy: busy, but flat.
			series("localhost:9100", 2, func(i int) float64 { return 9 + jitter(i) }),
		},
		CRDTJournalMetric: {
			// Compacted every 20 minutes: normal.
			series("cloute:8080", 3, func(i int) float64 { return 50*float64(i%20) + jitter(i) }),
			// Never compacted in the last hour.
			series("cloute:8080", 4, func(i int) float64 { return 500 + 50*float64(i) + jitter(i) }),
		},
	}}

	got, err := newEvaluator(store).Evaluate(t.Context(), evalTime)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	leak := find(t, got, "goroutine_leak", 1)
	if leak.Status != string(detect.StatusGrowing) || !leak.Firing || leak.Kind != string(KindGrowth) || leak.Slope*60 < 2.5 {
		t.Errorf("leak: %+v, want firing growth of about 3 per minute", leak)
	}
	if healthy := find(t, got, "goroutine_leak", 2); healthy.Firing {
		t.Errorf("flat goroutines flagged: %+v", healthy)
	}
	if compacted := find(t, got, "crdt_journal_growth", 3); compacted.Firing {
		t.Errorf("journal with regular compaction flagged: %+v", compacted)
	}
	if stuck := find(t, got, "crdt_journal_growth", 4); !stuck.Firing || stuck.Name != CRDTJournalMetric {
		t.Errorf("journal without compaction: %+v, want firing", stuck)
	}
}

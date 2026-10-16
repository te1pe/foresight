package clickhouse

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"github.com/te1pe/foresight/internal/scrape"
)

// Same version as deploy/docker-compose.yml.
const clickhouseImage = "clickhouse/clickhouse-server:26.3-alpine"

var testDSN string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("skipping clickhouse integration tests in -short mode")
		os.Exit(0)
	}
	os.Exit(runWithClickHouse(m))
}

func runWithClickHouse(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcclickhouse.Run(ctx, clickhouseImage,
		tcclickhouse.WithUsername("foresight"),
		tcclickhouse.WithPassword("foresight"),
		tcclickhouse.WithDatabase("foresight"),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintln(os.Stderr, "terminate container:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start clickhouse container:", err)
		return 1
	}
	testDSN, err = ctr.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection string:", err)
		return 1
	}
	if err := migrate(ctx, testDSN); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

// migrate applies the real migrations, so the tests cover the schema too.
func migrate(ctx context.Context, dsn string) error {
	opts, err := ch.ParseDSN(dsn)
	if err != nil {
		return err
	}
	db := ch.OpenDB(opts)
	defer db.Close()

	p, err := goose.NewProvider(goose.DialectClickHouse, db, os.DirFS("../../../migrations/clickhouse"))
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(testDSN)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Tests isolate themselves by target rather than by TRUNCATE: the
// deduplication log is not cleared by TRUNCATE, so reusing a target and
// timestamp across tests would make inserts silently disappear.
func target(t *testing.T) string { return t.Name() }

var base = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

// scrapeAt builds one scrape with one sample per value in vals, keyed by
// mountpoint label.
func scrapeAt(tgt string, offset time.Duration, vals map[string]float64) scrape.Batch {
	b := scrape.Batch{Target: tgt, Time: base.Add(offset)}
	for mp, v := range vals {
		b.Samples = append(b.Samples, scrape.Sample{
			Name: "node_filesystem_avail_bytes", Labels: map[string]string{"mountpoint": mp},
			Value: v, Type: scrape.TypeGauge,
		})
	}
	return b
}

func query(t *testing.T, s *Store, tgt string, step Step, labels map[string]string) []Series {
	t.Helper()
	got, err := s.QuerySeries(t.Context(), SeriesQuery{
		Target: tgt, Name: "node_filesystem_avail_bytes", Labels: labels,
		From: base, To: base.Add(time.Hour), Step: step, MaxPoints: 1000,
	})
	if err != nil {
		t.Fatalf("QuerySeries(%s): %v", step, err)
	}
	return got
}

func TestSeriesID(t *testing.T) {
	a := seriesID("t", "m", map[string]string{"x": "1", "y": "2"})
	if b := seriesID("t", "m", map[string]string{"y": "2", "x": "1"}); a != b {
		t.Error("label order changes the series id")
	}
	if seriesID("t", "m", map[string]string{"ab": "c"}) == seriesID("t", "m", map[string]string{"a": "bc"}) {
		t.Error("ambiguous concatenation collides")
	}
	if seriesID("t1", "m", nil) == seriesID("t2", "m", nil) {
		t.Error("target is not part of the series id")
	}
}

func TestRawRoundTripAndLabelFilter(t *testing.T) {
	s := newTestStore(t)
	tgt := target(t)
	_, err := s.InsertBatches(t.Context(), []scrape.Batch{
		scrapeAt(tgt, 5*time.Second, map[string]float64{"/": 100, "/data": math.Inf(1)}),
		scrapeAt(tgt, 20*time.Second+123*time.Millisecond, map[string]float64{"/": 90, "/data": math.NaN()}),
	})
	if err != nil {
		t.Fatalf("InsertBatches: %v", err)
	}

	all := query(t, s, tgt, StepRaw, nil)
	if len(all) != 2 {
		t.Fatalf("got %d series, want 2", len(all))
	}

	root := query(t, s, tgt, StepRaw, map[string]string{"mountpoint": "/"})
	if len(root) != 1 || len(root[0].Points) != 2 {
		t.Fatalf("filtered query: %+v", root)
	}
	sr := root[0]
	if sr.Labels["mountpoint"] != "/" || sr.Type != "gauge" || sr.ID != seriesID(tgt, "node_filesystem_avail_bytes", sr.Labels) {
		t.Errorf("series metadata: %+v", sr)
	}
	// Millisecond precision survives; that is the column's resolution.
	if p := sr.Points[1]; !p.Time.Equal(base.Add(20*time.Second+123*time.Millisecond)) || p.Value != 90 {
		t.Errorf("second point = %+v", p)
	}

	data := query(t, s, tgt, StepRaw, map[string]string{"mountpoint": "/data"})
	if len(data) != 1 || !math.IsInf(data[0].Points[0].Value, 1) || !math.IsNaN(data[0].Points[1].Value) {
		t.Errorf("special values: %+v", data)
	}
}

func TestWindows(t *testing.T) {
	s := newTestStore(t)
	tgt := target(t)
	var batches []scrape.Batch
	for _, p := range []struct {
		at time.Duration
		v  float64
	}{
		{5 * time.Second, 10}, {20 * time.Second, 30}, {50 * time.Second, 20}, // 12:00
		{70 * time.Second, 40},              // 12:01
		{4*time.Minute + 59*time.Second, 5}, // 12:04
	} {
		batches = append(batches, scrapeAt(tgt, p.at, map[string]float64{"/": p.v}))
	}
	if _, err := s.InsertBatches(t.Context(), batches); err != nil {
		t.Fatalf("InsertBatches: %v", err)
	}

	m1 := query(t, s, tgt, Step1m, nil)
	if len(m1) != 1 {
		t.Fatalf("1m: got %d series", len(m1))
	}
	want1m := []Window{
		{Start: base, Min: 10, Max: 30, Avg: 20, Last: 20, Count: 3},
		{Start: base.Add(time.Minute), Min: 40, Max: 40, Avg: 40, Last: 40, Count: 1},
		{Start: base.Add(4 * time.Minute), Min: 5, Max: 5, Avg: 5, Last: 5, Count: 1},
	}
	assertWindows(t, "1m", m1[0].Windows, want1m)

	m5 := query(t, s, tgt, Step5m, nil)
	assertWindows(t, "5m", m5[0].Windows, []Window{
		{Start: base, Min: 5, Max: 40, Avg: 21, Last: 5, Count: 5},
	})
	if m5[0].Labels["mountpoint"] != "/" {
		t.Errorf("5m labels = %v", m5[0].Labels)
	}
}

func assertWindows(t *testing.T, step string, got, want []Window) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d windows %+v, want %d", step, len(got), got, len(want))
	}
	for i := range want {
		if !got[i].Start.Equal(want[i].Start) || got[i].Min != want[i].Min || got[i].Max != want[i].Max ||
			got[i].Avg != want[i].Avg || got[i].Last != want[i].Last || got[i].Count != want[i].Count {
			t.Errorf("%s window %d = %+v, want %+v", step, i, got[i], want[i])
		}
	}
}

// The core property of this stage: at-least-once delivery from the queue
// must not change stored data, neither raw points nor window aggregates.
func TestRedeliveryChangesNothing(t *testing.T) {
	s := newTestStore(t)
	tgt := target(t)
	first := scrapeAt(tgt, 5*time.Second, map[string]float64{"/": 10})
	second := scrapeAt(tgt, 20*time.Second, map[string]float64{"/": 30})

	if _, err := s.InsertBatches(t.Context(), []scrape.Batch{first}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Redelivered as part of a different poll: grouped with a new scrape,
	// and duplicated within the same call.
	if _, err := s.InsertBatches(t.Context(), []scrape.Batch{first, second, second}); err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	var raw uint64
	if err := s.conn.QueryRow(t.Context(), "SELECT count() FROM samples WHERE target = ?", tgt).Scan(&raw); err != nil {
		t.Fatalf("count raw: %v", err)
	}
	if raw != 2 {
		t.Errorf("raw rows without FINAL = %d, want 2: duplicates reached the table", raw)
	}
	for _, step := range []Step{Step1m, Step5m} {
		w := query(t, s, tgt, step, nil)[0].Windows
		assertWindows(t, string(step), w, []Window{{Start: base, Min: 10, Max: 30, Avg: 20, Last: 30, Count: 2}})
	}
}

func TestTooManyPoints(t *testing.T) {
	s := newTestStore(t)
	tgt := target(t)
	var batches []scrape.Batch
	for i := range 5 {
		batches = append(batches, scrapeAt(tgt, time.Duration(i)*time.Second, map[string]float64{"/": float64(i)}))
	}
	if _, err := s.InsertBatches(t.Context(), batches); err != nil {
		t.Fatalf("insert: %v", err)
	}

	q := SeriesQuery{Target: tgt, Name: "node_filesystem_avail_bytes", From: base, To: base.Add(time.Hour), Step: StepRaw, MaxPoints: 5}
	if _, err := s.QuerySeries(t.Context(), q); err != nil {
		t.Fatalf("exactly MaxPoints must pass: %v", err)
	}
	q.MaxPoints = 4
	if _, err := s.QuerySeries(t.Context(), q); !errors.Is(err, ErrTooManyPoints) {
		t.Errorf("err = %v, want ErrTooManyPoints", err)
	}
}

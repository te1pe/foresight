package postgres

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/te1pe/foresight/internal/scrape"
)

// Same major version as deploy/docker-compose.yml.
const postgresImage = "postgres:18-alpine"

var testDSN string

// One container for the whole package: starting PostgreSQL dominates the
// test time, and tests isolate themselves by truncating the table.
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("skipping postgres integration tests in -short mode")
		os.Exit(0)
	}
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("foresight"),
		tcpostgres.WithUsername("foresight"),
		tcpostgres.WithPassword("foresight"),
		tcpostgres.BasicWaitStrategies(),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintln(os.Stderr, "terminate container:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres container:", err)
		return 1
	}

	testDSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
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

// migrate applies the real migrations, so the test also proves they work.
func migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	p, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../../migrations"))
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
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(t.Context(), "TRUNCATE samples"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

func TestInsertBatches(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	// Microsecond precision: that is what timestamptz keeps.
	t1 := time.Date(2026, 10, 15, 12, 0, 0, 123456000, time.UTC)
	t2 := t1.Add(15 * time.Second)
	batches := []scrape.Batch{
		{Target: "localhost:9100", Time: t1, Samples: []scrape.Sample{
			{Name: "node_filesystem_avail_bytes", Labels: map[string]string{"mountpoint": "/", "note": "a \"quoted\"\nvalue"}, Value: 2.05e11, Type: scrape.TypeGauge},
			{Name: "up", Value: 1, Type: scrape.TypeGauge},
		}},
		{Target: "localhost:9999", Time: t2, Samples: []scrape.Sample{
			{Name: "h_bucket", Labels: map[string]string{"le": "+Inf"}, Value: math.Inf(1), Type: scrape.TypeHistogram},
			{Name: "nan_metric", Value: math.NaN(), Type: scrape.TypeUntyped},
			{Name: "neg", Value: math.Inf(-1), Type: scrape.TypeUntyped},
		}},
		{Target: "empty", Time: t2},
	}

	res, err := s.InsertBatches(ctx, batches)
	if err != nil {
		t.Fatalf("InsertBatches: %v", err)
	}
	if res != (InsertResult{Copied: 5, Inserted: 5}) {
		t.Fatalf("result = %+v, want 5 copied and 5 inserted", res)
	}

	var (
		ts         time.Time
		mountpoint string
		note       string
		value      float64
		typ        string
	)
	err = s.pool.QueryRow(ctx, `
		SELECT ts, labels->>'mountpoint', labels->>'note', value, type
		FROM samples WHERE target = 'localhost:9100' AND name = 'node_filesystem_avail_bytes'`,
	).Scan(&ts, &mountpoint, &note, &value, &typ)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if !ts.Equal(t1) || mountpoint != "/" || note != "a \"quoted\"\nvalue" || value != 2.05e11 || typ != "gauge" {
		t.Errorf("got ts=%v mountpoint=%q note=%q value=%v type=%q", ts, mountpoint, note, value, typ)
	}

	var labelsJSON string
	if err := s.pool.QueryRow(ctx, `SELECT labels::text FROM samples WHERE name = 'up'`).Scan(&labelsJSON); err != nil {
		t.Fatalf("select up: %v", err)
	}
	if labelsJSON != "{}" {
		t.Errorf("labels for a sample without labels = %s, want {}", labelsJSON)
	}

	var inf, nan, negInf float64
	err = s.pool.QueryRow(ctx, `
		SELECT
			(SELECT value FROM samples WHERE name = 'h_bucket'),
			(SELECT value FROM samples WHERE name = 'nan_metric'),
			(SELECT value FROM samples WHERE name = 'neg')`,
	).Scan(&inf, &nan, &negInf)
	if err != nil {
		t.Fatalf("select special values: %v", err)
	}
	if !math.IsInf(inf, 1) || !math.IsNaN(nan) || !math.IsInf(negInf, -1) {
		t.Errorf("special values round-trip: +Inf=%v NaN=%v -Inf=%v", inf, nan, negInf)
	}
}

func countRows(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM samples").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestInsertBatchesIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	first := scrape.Batch{Target: "t", Time: at, Samples: []scrape.Sample{
		{Name: "m", Labels: map[string]string{"a": "1", "b": "2"}, Value: 1, Type: scrape.TypeGauge},
		{Name: "m", Labels: map[string]string{"a": "1", "b": "3"}, Value: 2, Type: scrape.TypeGauge},
		{Name: "up", Value: 1, Type: scrape.TypeGauge},
	}}
	if _, err := s.InsertBatches(ctx, []scrape.Batch{first}); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// A redelivery of the same batch, together with a genuinely new scrape
	// and a duplicate inside the same call.
	next := scrape.Batch{Target: "t", Time: at.Add(15 * time.Second), Samples: []scrape.Sample{
		{Name: "up", Value: 1, Type: scrape.TypeGauge},
	}}
	res, err := s.InsertBatches(ctx, []scrape.Batch{first, next, next})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res != (InsertResult{Copied: 5, Inserted: 1}) {
		t.Errorf("replay result = %+v, want 5 copied and only the new scrape inserted", res)
	}
	if n := countRows(t, s); n != 4 {
		t.Errorf("rows = %d, want 4", n)
	}

	// Label order on the wire must not matter: jsonb normalizes key order,
	// so the same series written with keys swapped is still a duplicate.
	swapped := scrape.Batch{Target: "t", Time: at, Samples: []scrape.Sample{
		{Name: "m", Labels: map[string]string{"b": "2", "a": "1"}, Value: 1, Type: scrape.TypeGauge},
	}}
	if res, err := s.InsertBatches(ctx, []scrape.Batch{swapped}); err != nil || res.Inserted != 0 {
		t.Errorf("same series again: result %+v, err %v; want nothing inserted", res, err)
	}
}

func TestInsertBatchesRejectsBadDataPermanently(t *testing.T) {
	s := newTestStore(t)

	// jsonb cannot store U+0000; a target exposing it would otherwise make
	// the consumer retry this batch forever.
	bad := scrape.Batch{Target: "t", Time: time.Now(), Samples: []scrape.Sample{
		{Name: "m", Labels: map[string]string{"a": "x\x00y"}, Value: 1, Type: scrape.TypeGauge},
	}}
	_, err := s.InsertBatches(t.Context(), []scrape.Batch{bad})
	if err == nil {
		t.Fatal("InsertBatches accepted a NUL byte in jsonb")
	}
	if !IsPermanent(err) {
		t.Errorf("IsPermanent(%v) = false, want true", err)
	}
	if n := countRows(t, s); n != 0 {
		t.Errorf("rows = %d after a failed write, want 0: the transaction must leave nothing behind", n)
	}
}

func TestIsPermanentForTransientErrors(t *testing.T) {
	s, err := New("postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	_, err = s.InsertBatches(t.Context(), []scrape.Batch{{Target: "t", Time: time.Now()}})
	if err == nil {
		t.Fatal("InsertBatches succeeded against an unreachable database")
	}
	if IsPermanent(err) {
		t.Errorf("connection failure classified as permanent: %v", err)
	}
}

func TestPingFailsWhenDatabaseIsUnreachable(t *testing.T) {
	// Port 1 is reserved and nothing listens there.
	s, err := New("postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping succeeded against an unreachable database")
	}
}

// Package postgres holds the PostgreSQL side: incidents and their
// notification outbox, plus the sample writer of stage 2 (kept, unused
// since samples moved to ClickHouse).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/te1pe/foresight/internal/scrape"
)

type Store struct {
	pool *pgxpool.Pool
}

// New does not connect: pgxpool dials lazily, which lets the service start
// while the database is still down and report that through /readyz.
// An error here means the DSN itself is invalid.
func New(dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}

func (s *Store) Close() { s.pool.Close() }

var sampleColumns = []string{"ts", "target", "name", "labels", "value", "type"}

type InsertResult struct {
	Copied   int64 // rows received
	Inserted int64 // rows that were new; Copied-Inserted were already stored
}

// InsertBatches is idempotent: writing the same batches again inserts
// nothing. That is what makes at-least-once delivery from the queue safe.
//
// COPY is the fast bulk path but has no ON CONFLICT, so rows are copied into
// a transaction-local staging table and moved with INSERT ... SELECT, which
// does. The whole write is one transaction: a failure leaves no partial
// batch behind, and the redelivered batch starts from a clean state.
func (s *Store) InsertBatches(ctx context.Context, batches []scrape.Batch) (InsertResult, error) {
	var res InsertResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// ON COMMIT DROP: the table lives only as long as this transaction,
		// so pooled connections never see a leftover from a previous write.
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE samples_stage (LIKE samples) ON COMMIT DROP`); err != nil {
			return fmt.Errorf("create staging table: %w", err)
		}
		n, err := tx.CopyFrom(ctx, pgx.Identifier{"samples_stage"}, sampleColumns, &batchRows{batches: batches})
		if err != nil {
			return fmt.Errorf("copy samples: %w", err)
		}
		res.Copied = n

		// No conflict target: DO NOTHING then applies to the expression
		// unique index as well, and to duplicates within this same batch.
		tag, err := tx.Exec(ctx, `
			INSERT INTO samples (ts, target, name, labels, value, type)
			SELECT ts, target, name, labels, value, type FROM samples_stage
			ON CONFLICT DO NOTHING`)
		if err != nil {
			return fmt.Errorf("move staged samples: %w", err)
		}
		res.Inserted = tag.RowsAffected()
		return nil
	})
	if err != nil {
		return InsertResult{}, fmt.Errorf("insert samples: %w", err)
	}
	return res, nil
}

// IsPermanent reports errors caused by the data itself, so retrying the same
// batch is pointless: class 22 (data exception, e.g. a NUL byte that jsonb
// cannot store) and class 23 (integrity constraint violation). Everything
// else — a lost connection, a missing table before migrations — may succeed
// later and is worth retrying.
func IsPermanent(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23")
}

// batchRows feeds COPY straight from the scrape batches, so no intermediate
// [][]any of the whole batch is ever built.
type batchRows struct {
	batches []scrape.Batch
	b, i    int // current batch and the sample within it; i is one past the current row
	row     [6]any
}

var emptyLabels = map[string]string{}

func (r *batchRows) Next() bool {
	for r.b < len(r.batches) {
		batch := r.batches[r.b]
		if r.i < len(batch.Samples) {
			smp := batch.Samples[r.i]
			r.i++
			labels := smp.Labels
			if labels == nil {
				// Store "{}" rather than JSON null so queries like
				// labels->>'mountpoint' never have to special-case null.
				labels = emptyLabels
			}
			r.row = [6]any{batch.Time, batch.Target, smp.Name, labels, smp.Value, string(smp.Type)}
			return true
		}
		r.b, r.i = r.b+1, 0
	}
	return false
}

func (r *batchRows) Values() ([]any, error) { return r.row[:], nil }

func (r *batchRows) Err() error { return nil }

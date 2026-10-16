package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/te1pe/foresight/internal/logs"
)

// InsertLogs writes all entries of all batches in one INSERT. No
// deduplication token is needed: the table's sorting key is the line's
// identity and ReplacingMergeTree collapses repeats.
func (s *Store) InsertLogs(ctx context.Context, batches []logs.Batch) (rows int, err error) {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO logs (ts, service, level, message, attrs, raw, received_at)")
	if err != nil {
		return 0, fmt.Errorf("insert logs: prepare: %w", err)
	}
	for _, b := range batches {
		for _, e := range b.Entries {
			attrs := e.Attrs
			if attrs == nil {
				attrs = map[string]string{}
			}
			if err := batch.Append(e.Time, b.Service, e.Level, e.Message, attrs, e.Raw, b.Received); err != nil {
				_ = batch.Abort()
				return 0, fmt.Errorf("insert logs of %s: append: %w", b.Service, err)
			}
			rows++
		}
	}
	if rows == 0 {
		_ = batch.Abort()
		return 0, nil
	}
	start := time.Now()
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("insert logs: send: %w", err)
	}
	insertDuration.WithLabelValues("logs").Observe(time.Since(start).Seconds())
	insertedRows.WithLabelValues("logs").Add(float64(rows))
	for _, b := range batches {
		pipelineLatency.WithLabelValues("logs").Observe(time.Since(b.Received).Seconds())
	}
	return rows, nil
}

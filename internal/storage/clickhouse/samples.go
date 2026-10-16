// Package clickhouse stores samples in ClickHouse and reads them back as
// series of raw points or pre-aggregated windows.
package clickhouse

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/te1pe/foresight/internal/scrape"
)

type Store struct {
	conn driver.Conn
}

// New does not connect: the driver dials on first use, so a service can
// start while ClickHouse is still down. An error means a bad DSN.
func New(dsn string) (*Store, error) {
	opts, err := ch.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	conn, err := ch.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	return &Store{conn: conn}, nil
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}
	return nil
}

func (s *Store) Close() error { return s.conn.Close() }

// InsertBatches writes each scrape as its own INSERT carrying a
// deduplication token derived from the scrape itself. A redelivered scrape
// gets the same token and is dropped by ClickHouse, in the raw table and in
// the window views alike, no matter how the consumer happened to group
// records into polls. If a call fails halfway, retrying it is safe: the
// scrapes already written are skipped.
//
// The inserts run one after another, and that is the measured throughput
// ceiling of the processor: about 10 scrapes per second (~5k rows/s at
// 540 samples a scrape), latency-bound, at 0.02 cores. Running them in
// parallel was tried and reverted: under load, on a database with live
// parts and merges, ClickHouse 26.3 failed concurrent inserts into the 1m
// window view with "LOGICAL_ERROR: Invalid partition key size: 0", the same
// records went through sequentially, and a fresh test container did not
// reproduce it. A state-dependent server error that wedges a partition is
// worse than a known ceiling. Raising it means fewer, larger inserts, which
// needs a deduplication key that survives regrouping (see README, "Нагрузка").
func (s *Store) InsertBatches(ctx context.Context, batches []scrape.Batch) (rows int, err error) {
	for _, b := range batches {
		if len(b.Samples) == 0 {
			continue
		}
		start := time.Now()
		if err := s.insertScrape(ctx, b); err != nil {
			return rows, err
		}
		insertDuration.WithLabelValues("samples").Observe(time.Since(start).Seconds())
		insertedRows.WithLabelValues("samples").Add(float64(len(b.Samples)))
		pipelineLatency.WithLabelValues("samples").Observe(time.Since(b.Time).Seconds())
		rows += len(b.Samples)
	}
	return rows, nil
}

func (s *Store) insertScrape(ctx context.Context, b scrape.Batch) error {
	// Every sample of a scrape shares one timestamp, so the INSERT lands in
	// one partition as exactly one block: the token then identifies the
	// whole scrape, not a fragment of it.
	ctx = ch.Context(ctx, ch.WithSettings(ch.Settings{
		"insert_deduplication_token": dedupToken(b),
		// Without this ClickHouse 26.3 drops the duplicate block from the
		// raw table but still pushes it into the window views (verified
		// against the server, see the integration test).
		"deduplicate_blocks_in_dependent_materialized_views": 1,
	}))

	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO samples (ts, target, name, series_id, labels, value, type)")
	if err != nil {
		return fmt.Errorf("insert scrape of %s: prepare: %w", b.Target, err)
	}
	for _, smp := range b.Samples {
		labels := smp.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		err := batch.Append(b.Time, b.Target, smp.Name, seriesID(b.Target, smp.Name, labels), labels, smp.Value, string(smp.Type))
		if err != nil {
			_ = batch.Abort()
			return fmt.Errorf("insert scrape of %s: append %s: %w", b.Target, smp.Name, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert scrape of %s: send: %w", b.Target, err)
	}
	return nil
}

// dedupToken identifies a scrape: a target is never scraped twice at the
// same instant, and the time survives the queue with full precision.
func dedupToken(b scrape.Batch) string {
	return b.Target + "|" + strconv.FormatInt(b.Time.UnixNano(), 10)
}

// seriesID hashes the series identity. Labels are sorted because map order
// is random and the same series must always get the same id. Each field is
// length-prefixed so that ("ab","c") and ("a","bc") cannot hash alike.
//
// 64-bit FNV-1a: a collision needs two series with the same target and
// name, which makes it vanishingly unlikely at this cardinality.
func seriesID(target, name string, labels map[string]string) uint64 {
	h := fnv.New64a()
	var lenBuf [binary.MaxVarintLen64]byte
	write := func(s string) {
		n := binary.PutUvarint(lenBuf[:], uint64(len(s)))
		h.Write(lenBuf[:n])
		h.Write([]byte(s))
	}

	write(target)
	write(name)
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		write(k)
		write(labels[k])
	}
	return h.Sum64()
}

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// Step selects the resolution of a series query.
type Step string

const (
	StepRaw Step = "raw"
	Step1m  Step = "1m"
	Step5m  Step = "5m"
)

// windowTables maps a step to its table and window length. The table name
// comes from this fixed map, never from user input.
var windowTables = map[Step]struct {
	table string
	size  time.Duration
}{
	Step1m: {"samples_1m", time.Minute},
	Step5m: {"samples_5m", 5 * time.Minute},
}

func (s Step) Valid() bool {
	_, ok := windowTables[s]
	return ok || s == StepRaw
}

type SeriesQuery struct {
	// Target may be empty to query the metric across all targets.
	Target string
	Name   string
	// Labels are exact-match filters; a series must have all of them.
	Labels   map[string]string
	From, To time.Time // [From, To)
	Step     Step
	// MaxPoints caps the rows read across all series.
	MaxPoints int
}

type Point struct {
	Time  time.Time
	Value float64
}

type Window struct {
	Start               time.Time
	Min, Max, Avg, Last float64
	Count               uint64
}

// Series holds Points for StepRaw and Windows for the aggregated steps.
type Series struct {
	Target  string
	ID      uint64
	Labels  map[string]string
	Type    string
	Points  []Point
	Windows []Window
}

// ErrTooManyPoints means the query would return more than MaxPoints rows;
// the caller should narrow the range, add label filters or use a coarser
// step rather than receive a silently truncated answer.
var ErrTooManyPoints = errors.New("too many points")

func (s *Store) QuerySeries(ctx context.Context, q SeriesQuery) ([]Series, error) {
	if !q.Step.Valid() {
		return nil, fmt.Errorf("unknown step %q", q.Step)
	}
	if q.MaxPoints <= 0 {
		return nil, errors.New("MaxPoints must be positive")
	}
	if q.Step == StepRaw {
		return s.queryRaw(ctx, q)
	}
	return s.queryWindows(ctx, q)
}

// seriesFilter renders the target, name and exact-match label conditions
// with placeholders; label keys are sorted so the same query always
// produces the same SQL text.
func seriesFilter(q SeriesQuery) (string, []any) {
	var sb strings.Builder
	sb.WriteString("name = ?")
	args := []any{q.Name}
	if q.Target != "" {
		sb.WriteString(" AND target = ?")
		args = append(args, q.Target)
	}
	for _, k := range slices.Sorted(maps.Keys(q.Labels)) {
		sb.WriteString(" AND labels[?] = ?")
		args = append(args, k, q.Labels[k])
	}
	return sb.String(), args
}

// Times are passed as Unix milliseconds: the driver's binding of time.Time
// is not guaranteed to keep sub-second precision.
func (s *Store) queryRaw(ctx context.Context, q SeriesQuery) ([]Series, error) {
	filter, filterArgs := seriesFilter(q)
	// FINAL applies ReplacingMergeTree deduplication at read time, so a
	// replay that outlived the insert-deduplication window is not visible
	// as doubled points even before the background merge catches up.
	sql := `
		SELECT target, series_id, labels, type, ts, value
		FROM samples FINAL
		WHERE ` + filter + `
		  AND ts >= fromUnixTimestamp64Milli(?, 'UTC') AND ts < fromUnixTimestamp64Milli(?, 'UTC')
		ORDER BY target, series_id, ts
		LIMIT ?`
	args := append(filterArgs, q.From.UnixMilli(), q.To.UnixMilli())
	args = append(args, q.MaxPoints+1)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query raw series %s/%s: %w", q.Target, q.Name, err)
	}
	defer rows.Close()

	var (
		out   []Series
		count int
	)
	for rows.Next() {
		var (
			target string
			id     uint64
			labels map[string]string
			typ    string
			p      Point
		)
		if err := rows.Scan(&target, &id, &labels, &typ, &p.Time, &p.Value); err != nil {
			return nil, fmt.Errorf("scan raw point: %w", err)
		}
		if count++; count > q.MaxPoints {
			return nil, ErrTooManyPoints
		}
		// Rows are ordered by series, so a new id starts a new series.
		if len(out) == 0 || out[len(out)-1].ID != id {
			out = append(out, Series{Target: target, ID: id, Labels: labels, Type: typ})
		}
		out[len(out)-1].Points = append(out[len(out)-1].Points, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read raw series %s/%s: %w", q.Target, q.Name, err)
	}
	return out, nil
}

func (s *Store) queryWindows(ctx context.Context, q SeriesQuery) ([]Series, error) {
	wt := windowTables[q.Step]
	filter, filterArgs := seriesFilter(q)
	// The window containing From is included whole: a 1m query from 12:00:30
	// starts at the 12:00 window rather than dropping it.
	from := q.From.Truncate(wt.size)

	// Parts are merged in the background, so one window may still be spread
	// over several rows: GROUP BY with the -Merge combinators finishes the
	// aggregation at read time and is correct either way.
	sql := `
		SELECT target, series_id, any(labels), any(type), window_start,
		       min(min), max(max), avgMerge(avg), argMaxMerge(last), sum(count)
		FROM ` + wt.table + `
		WHERE ` + filter + `
		  AND window_start >= fromUnixTimestamp64Milli(?, 'UTC') AND window_start < fromUnixTimestamp64Milli(?, 'UTC')
		GROUP BY target, series_id, window_start
		ORDER BY target, series_id, window_start
		LIMIT ?`
	args := append(filterArgs, from.UnixMilli(), q.To.UnixMilli())
	args = append(args, q.MaxPoints+1)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query %s series %s/%s: %w", q.Step, q.Target, q.Name, err)
	}
	defer rows.Close()

	var (
		out   []Series
		count int
	)
	for rows.Next() {
		var (
			target string
			id     uint64
			labels map[string]string
			typ    string
			w      Window
		)
		if err := rows.Scan(&target, &id, &labels, &typ, &w.Start, &w.Min, &w.Max, &w.Avg, &w.Last, &w.Count); err != nil {
			return nil, fmt.Errorf("scan window: %w", err)
		}
		if count++; count > q.MaxPoints {
			return nil, ErrTooManyPoints
		}
		if len(out) == 0 || out[len(out)-1].ID != id {
			out = append(out, Series{Target: target, ID: id, Labels: labels, Type: typ})
		}
		out[len(out)-1].Windows = append(out[len(out)-1].Windows, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s series %s/%s: %w", q.Step, q.Target, q.Name, err)
	}
	return out, nil
}

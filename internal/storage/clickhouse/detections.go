package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Detection is one evaluation of one rule on one series.
type Detection struct {
	EvalTime time.Time
	Rule     string
	Kind     string
	Status   string
	Firing   bool
	Target   string
	Name     string
	SeriesID uint64
	Labels   map[string]string
	Observed float64
	Expected float64
	Score    float64
	Slope    float64
	// ETA is set only for a forecast that reaches its limit.
	ETA    *time.Duration
	Points int
}

// InsertDetections writes one evaluation round in a single INSERT. No
// deduplication token is needed: the table is keyed by (rule, series,
// eval_time) and a repeated round produces the same keys.
func (s *Store) InsertDetections(ctx context.Context, ds []Detection) error {
	if len(ds) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO detections
		(eval_time, rule, kind, status, firing, target, name, series_id, labels,
		 observed, expected, score, slope, eta_seconds, points)`)
	if err != nil {
		return fmt.Errorf("insert detections: prepare: %w", err)
	}
	for _, d := range ds {
		var eta *float64
		if d.ETA != nil {
			sec := d.ETA.Seconds()
			eta = &sec
		}
		labels := d.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		err := batch.Append(d.EvalTime, d.Rule, d.Kind, d.Status, d.Firing, d.Target, d.Name, d.SeriesID, labels,
			d.Observed, d.Expected, d.Score, d.Slope, eta, uint32(d.Points))
		if err != nil {
			_ = batch.Abort()
			return fmt.Errorf("insert detections: append %s/%d: %w", d.Rule, d.SeriesID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert detections: send: %w", err)
	}
	return nil
}

// LatestDetections returns the most recent evaluation of every rule and
// series evaluated since the given time. An empty target means all targets.
func (s *Store) LatestDetections(ctx context.Context, target string, since time.Time) ([]Detection, error) {
	var where strings.Builder
	where.WriteString("eval_time >= fromUnixTimestamp64Milli(?, 'UTC')")
	args := []any{since.UnixMilli()}
	if target != "" {
		where.WriteString(" AND target = ?")
		args = append(args, target)
	}
	return s.queryDetections(ctx, `
		SELECT `+detectionColumns+`
		FROM detections FINAL
		WHERE `+where.String()+`
		ORDER BY target, rule, series_id, eval_time DESC
		LIMIT 1 BY target, rule, series_id`, args...)
}

// RecentDetections returns every evaluation since the given time, grouped by
// series and oldest first within each: the history the alerter needs to
// tell a sustained condition from a single blip.
func (s *Store) RecentDetections(ctx context.Context, since time.Time) ([]Detection, error) {
	return s.queryDetections(ctx, `
		SELECT `+detectionColumns+`
		FROM detections FINAL
		WHERE eval_time >= fromUnixTimestamp64Milli(?, 'UTC')
		ORDER BY target, rule, series_id, eval_time`, since.UnixMilli())
}

const detectionColumns = `eval_time, rule, kind, status, firing, target, name, series_id, labels,
		       observed, expected, score, slope, eta_seconds, points`

func (s *Store) queryDetections(ctx context.Context, sql string, args ...any) ([]Detection, error) {
	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query detections: %w", err)
	}
	defer rows.Close()

	var out []Detection
	for rows.Next() {
		var (
			d      Detection
			eta    *float64
			points uint32
		)
		if err := rows.Scan(&d.EvalTime, &d.Rule, &d.Kind, &d.Status, &d.Firing, &d.Target, &d.Name, &d.SeriesID,
			&d.Labels, &d.Observed, &d.Expected, &d.Score, &d.Slope, &eta, &points); err != nil {
			return nil, fmt.Errorf("scan detection: %w", err)
		}
		if eta != nil {
			dur := time.Duration(*eta * float64(time.Second))
			d.ETA = &dur
		}
		d.Points = int(points)
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read detections: %w", err)
	}
	return out, nil
}

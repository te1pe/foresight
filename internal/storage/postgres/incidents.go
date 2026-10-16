package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Incident struct {
	ID          int64
	Fingerprint string
	Rule        string
	Target      string
	SeriesID    string
	Labels      map[string]string
	Summary     string
	OpenedAt    time.Time
	LastEvalAt  time.Time
}

// Event is an outbox row: a notification that still has to be delivered.
type Event struct {
	ID         int64
	IncidentID int64
	Kind       string
	Message    string
	CreatedAt  time.Time
}

const (
	ResolveRecovered = "recovered"
	ResolveNoData    = "no_data"
)

func (s *Store) OpenIncidents(ctx context.Context) ([]Incident, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, fingerprint, rule, target, series_id, labels, summary, opened_at, last_eval_at
		FROM incidents WHERE status = 'firing' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query open incidents: %w", err)
	}
	incidents, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Incident, error) {
		var inc Incident
		err := row.Scan(&inc.ID, &inc.Fingerprint, &inc.Rule, &inc.Target, &inc.SeriesID,
			&inc.Labels, &inc.Summary, &inc.OpenedAt, &inc.LastEvalAt)
		return inc, err
	})
	if err != nil {
		return nil, fmt.Errorf("read open incidents: %w", err)
	}
	return incidents, nil
}

// OpenIncident creates the incident and its "opened" event in one
// transaction. opened is false when an incident with this fingerprint is
// already open, e.g. another alerter got there first; then nothing is
// written, so no duplicate notification is queued either.
func (s *Store) OpenIncident(ctx context.Context, inc Incident, message string) (opened bool, err error) {
	labels := inc.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx, `
			INSERT INTO incidents (fingerprint, rule, target, series_id, labels, status, summary, opened_at, last_eval_at)
			VALUES ($1, $2, $3, $4, $5, 'firing', $6, $7, $8)
			ON CONFLICT (fingerprint) WHERE status = 'firing' DO NOTHING
			RETURNING id`,
			inc.Fingerprint, inc.Rule, inc.Target, inc.SeriesID, labels, inc.Summary, inc.OpenedAt, inc.LastEvalAt,
		).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert incident: %w", err)
		}
		opened = true
		return insertEvent(ctx, tx, id, "opened", message)
	})
	if err != nil {
		return false, fmt.Errorf("open incident %s: %w", inc.Fingerprint, err)
	}
	return opened, nil
}

// ResolveIncident closes an open incident and queues its "resolved" event.
// resolved is false if it was not open anymore (resolved concurrently).
func (s *Store) ResolveIncident(ctx context.Context, id int64, at time.Time, reason, message string) (resolved bool, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE incidents SET status = 'resolved', resolved_at = $2, resolve_reason = $3
			WHERE id = $1 AND status = 'firing'`, id, at, reason)
		if err != nil {
			return fmt.Errorf("update incident: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		resolved = true
		return insertEvent(ctx, tx, id, "resolved", message)
	})
	if err != nil {
		return false, fmt.Errorf("resolve incident %d: %w", id, err)
	}
	return resolved, nil
}

// TouchIncident records that the incident is still firing, with its latest
// summary (an ETA moves every minute). It deliberately queues no event:
// a message per minute per incident would be noise.
func (s *Store) TouchIncident(ctx context.Context, id int64, lastEval time.Time, summary string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE incidents SET last_eval_at = $2, summary = $3
		WHERE id = $1 AND status = 'firing'`, id, lastEval, summary)
	if err != nil {
		return fmt.Errorf("touch incident %d: %w", id, err)
	}
	return nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, incidentID int64, kind, message string) error {
	_, err := tx.Exec(ctx, `INSERT INTO incident_events (incident_id, kind, message) VALUES ($1, $2, $3)`,
		incidentID, kind, message)
	if err != nil {
		return fmt.Errorf("insert %s event: %w", kind, err)
	}
	return nil
}

// PendingEvents returns undelivered events oldest first, so notifications
// go out in the order the transitions happened.
func (s *Store) PendingEvents(ctx context.Context, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, incident_id, kind, message, created_at
		FROM incident_events WHERE notified_at IS NULL
		ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending events: %w", err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Event])
	if err != nil {
		return nil, fmt.Errorf("read pending events: %w", err)
	}
	return events, nil
}

func (s *Store) MarkNotified(ctx context.Context, ids []int64, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE incident_events SET notified_at = $2
		WHERE id = ANY($1) AND notified_at IS NULL`, ids, at)
	if err != nil {
		return fmt.Errorf("mark %d events notified: %w", len(ids), err)
	}
	return nil
}

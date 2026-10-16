package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// EnabledTargets returns the targets ingest should scrape.
func (s *Store) EnabledTargets(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT target FROM targets WHERE enabled ORDER BY target`)
	if err != nil {
		return nil, fmt.Errorf("query targets: %w", err)
	}
	targets, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("read targets: %w", err)
	}
	return targets, nil
}

// AddTargets enables the given targets, creating them if needed.
func (s *Store) AddTargets(ctx context.Context, targets []string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO targets (target) SELECT unnest($1::text[])
		ON CONFLICT (target) DO UPDATE SET enabled = true`, targets)
	if err != nil {
		return fmt.Errorf("add %d targets: %w", len(targets), err)
	}
	return nil
}

// RemoveTargets deletes the given targets; unknown ones are ignored.
func (s *Store) RemoveTargets(ctx context.Context, targets []string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM targets WHERE target = ANY($1)`, targets); err != nil {
		return fmt.Errorf("remove %d targets: %w", len(targets), err)
	}
	return nil
}

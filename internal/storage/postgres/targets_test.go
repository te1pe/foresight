package postgres

import (
	"context"
	"slices"
	"testing"
)

// seeded is what the migrations put into targets: the former default
// (00004) and foresight's own admin listeners (00005).
var seeded = []string{"127.0.0.1:6060", "127.0.0.1:6061", "127.0.0.1:6062", "127.0.0.1:6063", "localhost:9100"}

func with(extra ...string) []string {
	return slices.Sorted(slices.Values(append(slices.Clone(seeded), extra...)))
}

func TestEnabledTargets(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	got, err := s.EnabledTargets(ctx)
	if err != nil {
		t.Fatalf("EnabledTargets: %v", err)
	}
	if !slices.Equal(got, seeded) {
		t.Fatalf("seeded targets = %v, want %v", got, seeded)
	}
	// Not t.Context(): it is already cancelled when cleanups run, and the
	// DELETE would silently fail, leaking rows into the next test.
	t.Cleanup(func() {
		if _, err := s.pool.Exec(context.Background(), "DELETE FROM targets WHERE NOT (target = ANY($1))", seeded); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})

	_, err = s.pool.Exec(ctx, `INSERT INTO targets (target, enabled) VALUES ('cloute:8080', true), ('old:9100', false)`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err = s.EnabledTargets(ctx)
	if err != nil {
		t.Fatalf("EnabledTargets: %v", err)
	}
	if !slices.Equal(got, with("cloute:8080")) {
		t.Errorf("targets = %v, want the enabled ones, sorted", got)
	}
}

func TestAddAndRemoveTargets(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	added := []string{"http://127.0.0.1:9300/t/0/metrics", "http://127.0.0.1:9300/t/1/metrics"}

	if err := s.AddTargets(ctx, added); err != nil {
		t.Fatalf("AddTargets: %v", err)
	}
	// Adding again re-enables instead of failing.
	s.pool.Exec(ctx, "UPDATE targets SET enabled = false WHERE target = $1", added[0])
	if err := s.AddTargets(ctx, added); err != nil {
		t.Fatalf("AddTargets again: %v", err)
	}
	got, _ := s.EnabledTargets(ctx)
	if !slices.Equal(got, with(added...)) {
		t.Errorf("after add: %v", got)
	}

	if err := s.RemoveTargets(ctx, append(added, "never-existed")); err != nil {
		t.Fatalf("RemoveTargets: %v", err)
	}
	if got, _ := s.EnabledTargets(ctx); !slices.Equal(got, seeded) {
		t.Errorf("after remove: %v", got)
	}
}

package postgres

import (
	"sync"
	"testing"
	"time"
)

func newIncidentStore(t *testing.T) *Store {
	t.Helper()
	s := newTestStore(t)
	if _, err := s.pool.Exec(t.Context(), "TRUNCATE incident_events, incidents RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

var at = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func incident(fp string) Incident {
	return Incident{
		Fingerprint: fp, Rule: "disk_fill_forecast", Target: "localhost:9100", SeriesID: "18446744073709551615",
		Labels: map[string]string{"mountpoint": "/"}, Summary: "full in 3h", OpenedAt: at, LastEvalAt: at,
	}
}

func TestIncidentLifecycle(t *testing.T) {
	s := newIncidentStore(t)
	ctx := t.Context()

	opened, err := s.OpenIncident(ctx, incident("fp1"), "opened fp1")
	if err != nil || !opened {
		t.Fatalf("open: %v, %v", opened, err)
	}
	// Opening again while it is open is a no-op, and queues nothing.
	if again, err := s.OpenIncident(ctx, incident("fp1"), "opened fp1 again"); err != nil || again {
		t.Fatalf("second open: %v, %v; want no-op", again, err)
	}

	open, err := s.OpenIncidents(ctx)
	if err != nil || len(open) != 1 {
		t.Fatalf("open incidents: %v, %+v", err, open)
	}
	inc := open[0]
	if inc.SeriesID != "18446744073709551615" || inc.Labels["mountpoint"] != "/" || !inc.OpenedAt.Equal(at) {
		t.Errorf("incident round trip: %+v", inc)
	}

	if err := s.TouchIncident(ctx, inc.ID, at.Add(time.Minute), "full in 2h59m"); err != nil {
		t.Fatalf("touch: %v", err)
	}
	if resolved, err := s.ResolveIncident(ctx, inc.ID, at.Add(5*time.Minute), ResolveRecovered, "resolved fp1"); err != nil || !resolved {
		t.Fatalf("resolve: %v, %v", resolved, err)
	}
	if again, err := s.ResolveIncident(ctx, inc.ID, at.Add(6*time.Minute), ResolveRecovered, "resolved twice"); err != nil || again {
		t.Fatalf("second resolve: %v, %v; want no-op", again, err)
	}

	// Once resolved, the same fingerprint may open a new incident.
	if reopened, err := s.OpenIncident(ctx, incident("fp1"), "opened fp1 later"); err != nil || !reopened {
		t.Fatalf("reopen: %v, %v", reopened, err)
	}

	events, err := s.PendingEvents(ctx, 10)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	var got []string
	for _, e := range events {
		got = append(got, e.Kind+":"+e.Message)
	}
	want := []string{"opened:opened fp1", "resolved:resolved fp1", "opened:opened fp1 later"}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %q, want %q", i, got[i], want[i])
		}
	}

	if err := s.MarkNotified(ctx, []int64{events[0].ID, events[1].ID}, at); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if left, _ := s.PendingEvents(ctx, 10); len(left) != 1 || left[0].ID != events[2].ID {
		t.Errorf("pending after marking = %+v, want only the last event", left)
	}
}

// Two alerters racing to open the same incident: the partial unique index
// lets exactly one win, so exactly one notification is queued.
func TestConcurrentOpenCreatesOneIncident(t *testing.T) {
	s := newIncidentStore(t)
	const racers = 8

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		winner int
	)
	for range racers {
		wg.Go(func() {
			opened, err := s.OpenIncident(t.Context(), incident("race"), "opened")
			if err != nil {
				t.Errorf("open: %v", err)
				return
			}
			if opened {
				mu.Lock()
				winner++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if winner != 1 {
		t.Errorf("%d racers opened the incident, want exactly 1", winner)
	}
	if events, _ := s.PendingEvents(t.Context(), 100); len(events) != 1 {
		t.Errorf("%d events queued, want 1", len(events))
	}
}

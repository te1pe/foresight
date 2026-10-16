package clickhouse

import (
	"testing"
	"time"

	"github.com/te1pe/foresight/internal/logs"
)

func TestInsertLogsCollapsesRedeliveries(t *testing.T) {
	s := newTestStore(t)
	service := t.Name()
	at := base.Add(1500 * time.Millisecond)
	b := logs.Batch{Service: service, Received: base, Entries: []logs.Entry{
		{Time: at, Level: "error", Message: "apply op", Attrs: map[string]string{"doc": "d1"}, Raw: `{"msg":"apply op","doc":"d1"}`},
		// Same instant, different line: a distinct entry, not a duplicate.
		{Time: at, Level: "info", Message: "retrying", Raw: `{"msg":"retrying"}`},
	}}

	if n, err := s.InsertLogs(t.Context(), []logs.Batch{b}); err != nil || n != 2 {
		t.Fatalf("insert: %d rows, %v", n, err)
	}
	// Redelivered by the queue, and resent by a client with a later
	// receive time: both must collapse into the original two lines.
	resent := b
	resent.Received = base.Add(time.Minute)
	if _, err := s.InsertLogs(t.Context(), []logs.Batch{b, resent}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	var rows uint64
	if err := s.conn.QueryRow(t.Context(), "SELECT count() FROM logs FINAL WHERE service = ?", service).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 2 {
		t.Errorf("rows under FINAL = %d, want 2", rows)
	}

	var (
		ts    time.Time
		level string
		doc   string
	)
	err := s.conn.QueryRow(t.Context(),
		"SELECT ts, level, attrs['doc'] FROM logs FINAL WHERE service = ? AND message = 'apply op'", service,
	).Scan(&ts, &level, &doc)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if !ts.Equal(at) || level != "error" || doc != "d1" {
		t.Errorf("got ts %v level %q doc %q", ts, level, doc)
	}

	if n, err := s.InsertLogs(t.Context(), []logs.Batch{{Service: service}}); err != nil || n != 0 {
		t.Errorf("empty batch: %d rows, %v", n, err)
	}
}

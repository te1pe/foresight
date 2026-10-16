package logs

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

var received = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func TestParseSlogOutput(t *testing.T) {
	// Real output of log/slog's JSON handler, not a hand-written imitation.
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	l.Error("apply op", "doc", "d1", "ops", 3, "err", "conflict", "client", map[string]any{"id": "c7", "v": 2})
	l.Info("ws connected")

	b, rejected, err := Parse(&buf, "cloute", received)
	if err != nil || rejected != 0 || len(b.Entries) != 2 {
		t.Fatalf("Parse: err %v, rejected %d, entries %d", err, rejected, len(b.Entries))
	}
	e := b.Entries[0]
	if e.Level != "error" || e.Message != "apply op" || e.Time.Equal(received) || e.Time.Location() != time.UTC {
		t.Errorf("entry = %+v", e)
	}
	want := map[string]string{"doc": "d1", "ops": "3", "err": "conflict", "client": `{"id":"c7","v":2}`}
	for k, v := range want {
		if e.Attrs[k] != v {
			t.Errorf("attr %s = %q, want %q", k, e.Attrs[k], v)
		}
	}
	if !strings.HasPrefix(e.Raw, "{") || strings.HasSuffix(e.Raw, "\n") {
		t.Errorf("raw = %q, want the line without its newline", e.Raw)
	}
	if b.Service != "cloute" || !b.Received.Equal(received) {
		t.Errorf("batch = %+v", b)
	}
}

func TestParseToleratesBadLines(t *testing.T) {
	input := strings.Join([]string{
		`{"ts":"2026-10-16T11:59:00Z","lvl":"WARN","message":"slow","ms":812}`,
		`not json at all`,
		``,
		`{"time":"yesterday","msg":"unparsable time"}`,
		`{}`,
		`["an","array"]`,
		`{"only":"attrs"}`,
	}, "\n")

	b, rejected, err := Parse(strings.NewReader(input), "svc", received)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if rejected != 3 || len(b.Entries) != 3 {
		t.Fatalf("rejected %d entries %d, want 3 and 3", rejected, len(b.Entries))
	}
	if e := b.Entries[0]; e.Level != "warn" || e.Message != "slow" || e.Attrs["ms"] != "812" ||
		!e.Time.Equal(time.Date(2026, 10, 16, 11, 59, 0, 0, time.UTC)) {
		t.Errorf("aliases: %+v", e)
	}
	if e := b.Entries[1]; !e.Time.Equal(received) {
		t.Errorf("unparsable time = %v, want the receive time", e.Time)
	}
	if e := b.Entries[2]; e.Message != "" || e.Attrs["only"] != "attrs" {
		t.Errorf("attrs-only line: %+v", e)
	}
}

func TestParseRejectsOverlongLine(t *testing.T) {
	long := `{"msg":"` + strings.Repeat("x", MaxLineSize) + `"}`
	_, _, err := Parse(strings.NewReader(long), "svc", received)
	if err == nil || !strings.Contains(err.Error(), "longer than 64 KiB") {
		t.Errorf("err = %v, want a clear line-length error", err)
	}
}

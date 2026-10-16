package scrape

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const exposition = `# TYPE node_load1 gauge
node_load1 1.5
# TYPE node_filesystem_avail_bytes gauge
node_filesystem_avail_bytes{mountpoint="/"} 1e+11
broken line
`

func newTestScraper(t *testing.T, timeout time.Duration) (*Scraper, chan Batch) {
	t.Helper()
	out := make(chan Batch, 16)
	// Short interval: the first scrape waits for the target's offset within
	// it, so an hour-long interval would delay it by up to an hour.
	s, err := New(Config{Interval: 200 * time.Millisecond, Timeout: timeout}, out, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, out
}

// targetFor registers a target with the scraper's status table the way
// start does, without launching its goroutine.
func targetFor(t *testing.T, s *Scraper, name string) target {
	t.Helper()
	u, err := targetURL(name)
	if err != nil {
		t.Fatalf("targetURL: %v", err)
	}
	s.status[name] = TargetStatus{Target: name}
	return target{name: name, url: u}
}

func sampleValue(b Batch, name string) (float64, bool) {
	for _, s := range b.Samples {
		if s.Name == name {
			return s.Value, true
		}
	}
	return 0, false
}

func TestScrapeSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, exposition)
	}))
	defer srv.Close()

	s, _ := newTestScraper(t, time.Second)
	before := time.Now()
	b, ok := s.scrapeOnce(t.Context(), targetFor(t, s, srv.URL))
	if !ok {
		t.Fatal("scrapeOnce reported cancellation")
	}

	if b.Target != srv.URL {
		t.Errorf("Target = %q, want %q", b.Target, srv.URL)
	}
	if b.Time.Before(before) || b.Time.After(time.Now()) {
		t.Errorf("Time %v is outside the scrape window", b.Time)
	}
	// 2 parsed samples + up + scrape_duration_seconds; the broken line is skipped.
	if len(b.Samples) != 4 {
		t.Fatalf("got %d samples, want 4: %+v", len(b.Samples), b.Samples)
	}
	if v, _ := sampleValue(b, "up"); v != 1 {
		t.Errorf("up = %v, want 1", v)
	}
	if v, ok := sampleValue(b, "scrape_duration_seconds"); !ok || v <= 0 {
		t.Errorf("scrape_duration_seconds = %v (present: %v), want > 0", v, ok)
	}
	if v, _ := sampleValue(b, "node_filesystem_avail_bytes"); v != 1e11 {
		t.Errorf("node_filesystem_avail_bytes = %v", v)
	}

	st := s.Targets()[0]
	if !st.Up || st.Samples != 4 || st.ParseErrors != 1 || st.LastError != "" || !st.LastScrape.Equal(b.Time) {
		t.Errorf("status = %+v", st)
	}
}

func TestScrapeFailures(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "target returns 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "boom", http.StatusInternalServerError)
			},
			wantErr: "500",
		},
		{
			name: "target exceeds timeout",
			handler: func(w http.ResponseWriter, r *http.Request) {
				// Stalls until the scraper gives up; the bound keeps a broken
				// test from hanging srv.Close forever.
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			},
			wantErr: "context deadline exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			const timeout = 100 * time.Millisecond
			s, _ := newTestScraper(t, timeout)
			b, ok := s.scrapeOnce(t.Context(), targetFor(t, s, srv.URL))
			if !ok {
				t.Fatal("scrapeOnce reported cancellation")
			}

			if len(b.Samples) != 2 {
				t.Fatalf("got %d samples, want only up and scrape_duration_seconds", len(b.Samples))
			}
			if v, _ := sampleValue(b, "up"); v != 0 {
				t.Errorf("up = %v, want 0", v)
			}
			if d, _ := sampleValue(b, "scrape_duration_seconds"); d > (timeout + time.Second).Seconds() {
				t.Errorf("scrape took %vs, timeout was not enforced", d)
			}

			st := s.Targets()[0]
			if st.Up || !strings.Contains(st.LastError, tt.wantErr) || st.LastErrorAt.IsZero() {
				t.Errorf("status = %+v, want down with error containing %q", st, tt.wantErr)
			}
		})
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "m 1\n")
	}))
	defer srv.Close()

	s, out := newTestScraper(t, time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	updates := make(chan []string, 1)
	updates <- []string{srv.URL}
	done := make(chan struct{})
	go func() {
		s.Run(ctx, updates)
		close(done)
	}()

	// The first scrape happens at the target's offset, within one interval.
	select {
	case b := <-out:
		if v, _ := sampleValue(b, "up"); v != 1 {
			t.Errorf("up = %v, want 1", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no batch from the first scrape")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestCancelledScrapeIsNotRecordedAsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	s, _ := newTestScraper(t, 5*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if _, ok := s.scrapeOnce(ctx, targetFor(t, s, srv.URL)); ok {
		t.Fatal("scrapeOnce returned a batch for a scrape aborted by shutdown")
	}
	if st := s.Targets()[0]; !st.LastScrape.IsZero() || st.LastError != "" {
		t.Errorf("status changed by an aborted scrape: %+v", st)
	}
}

func TestTargetURL(t *testing.T) {
	tests := []struct{ in, want string }{
		{"localhost:9100", "http://localhost:9100/metrics"},
		{"http://127.0.0.1:8080", "http://127.0.0.1:8080/metrics"},
		{"https://example.com/custom", "https://example.com/custom"},
	}
	for _, tt := range tests {
		got, err := targetURL(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("targetURL(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
	if _, err := targetURL("http://"); err == nil {
		t.Error("targetURL accepted an entry without a host")
	}
}

// exporter is a tiny metrics endpoint standing in for a real target.
func exporter(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "go_goroutines 8\n")
	}))
	t.Cleanup(srv.Close)
	return srv
}

func names(sts []TargetStatus) []string {
	out := make([]string, len(sts))
	for i, st := range sts {
		out[i] = st.Target
	}
	return out
}

// waitFor polls cond: the scraper reacts to updates asynchronously.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunReconcilesTargets(t *testing.T) {
	a, b := exporter(t), exporter(t)
	s, out := newTestScraper(t, time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	updates := make(chan []string)
	done := make(chan struct{})
	go func() {
		s.Run(ctx, updates)
		close(done)
	}()

	seen := func(target string) bool {
		for {
			select {
			case batch := <-out:
				if batch.Target == target {
					return true
				}
			default:
				return false
			}
		}
	}

	updates <- []string{a.URL}
	waitFor(t, "first target scraped", func() bool { return seen(a.URL) })

	// Adding a target starts it without touching the existing one.
	updates <- []string{a.URL, b.URL, "http://"}
	waitFor(t, "second target scraped", func() bool { return seen(b.URL) })
	if got := names(s.Targets()); !slices.Equal(got, slices.Sorted(slices.Values([]string{a.URL, b.URL}))) {
		t.Errorf("targets = %v; the invalid entry must be skipped, the rest kept", got)
	}

	// Removing a target stops it and drops its status.
	updates <- []string{b.URL}
	waitFor(t, "first target removed", func() bool { return slices.Equal(names(s.Targets()), []string{b.URL}) })

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if len(s.Targets()) != 0 {
		t.Errorf("targets after shutdown = %v", names(s.Targets()))
	}
}

func TestWatchTargets(t *testing.T) {
	var (
		mu    sync.Mutex
		lists = [][]string{{"b", "a", "a"}, {"a", "b"}, nil, {"c"}}
		errs  = []error{nil, nil, errors.New("db down"), nil}
		call  int
	)
	load := func(context.Context) ([]string, error) {
		mu.Lock()
		defer mu.Unlock()
		i := min(call, len(lists)-1)
		call++
		return lists[i], errs[i]
	}

	ctx, cancel := context.WithCancel(t.Context())
	out := make(chan []string, 10)
	done := make(chan struct{})
	go func() {
		WatchTargets(ctx, load, 10*time.Millisecond, out, slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(done)
	}()

	// Sorted and deduplicated; the identical second list and the failed
	// third load send nothing; the fourth is a real change.
	if got := <-out; !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("first list = %v", got)
	}
	select {
	case got := <-out:
		if !slices.Equal(got, []string{"c"}) {
			t.Errorf("second list = %v, want [c]: an unchanged list or a failed load must not be sent", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("change was not sent")
	}

	cancel()
	<-done
}

func TestScrapeOffset(t *testing.T) {
	const interval = 15 * time.Second
	if a, b := scrapeOffset("localhost:9100", interval), scrapeOffset("localhost:9100", interval); a != b {
		t.Error("offset is not stable for the same target")
	}

	// 1000 targets spread over 15 one-second slots: none should be crowded.
	slots := make([]int, 15)
	for i := range 1000 {
		off := scrapeOffset(fmt.Sprintf("http://127.0.0.1:9300/t/%d/metrics", i), interval)
		if off < 0 || off >= interval {
			t.Fatalf("offset %v outside [0, %v)", off, interval)
		}
		slots[off/time.Second]++
	}
	for i, n := range slots {
		// Expected ~67 per slot; a burst would show as one slot far above.
		if n < 30 || n > 110 {
			t.Errorf("slot %d holds %d of 1000 targets: offsets are not spread", i, n)
		}
	}
}

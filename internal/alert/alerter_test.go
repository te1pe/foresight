package alert

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/te1pe/foresight/internal/notify"
	chstore "github.com/te1pe/foresight/internal/storage/clickhouse"
	"github.com/te1pe/foresight/internal/storage/postgres"
	"github.com/te1pe/foresight/internal/storage/redis"
)

// The fakes reproduce the guarantees the real stores give (one open
// incident per fingerprint, atomic claims), because those guarantees are
// what the alerter's correctness rests on. Postgres and Redis themselves
// are covered by their own integration tests.

type fakeSource struct {
	mu   sync.Mutex
	rows []chstore.Detection
}

func (f *fakeSource) RecentDetections(_ context.Context, since time.Time) ([]chstore.Detection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []chstore.Detection
	for _, d := range f.rows {
		if !d.EvalTime.Before(since) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (f *fakeSource) add(ds ...chstore.Detection) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, ds...)
}

type fakeIncidents struct {
	mu        sync.Mutex
	incidents []postgres.Incident
	status    map[int64]string
	reasons   map[int64]string
	events    []postgres.Event
	notified  map[int64]bool
}

func newFakeIncidents() *fakeIncidents {
	return &fakeIncidents{status: map[int64]string{}, reasons: map[int64]string{}, notified: map[int64]bool{}}
}

func (f *fakeIncidents) OpenIncidents(context.Context) ([]postgres.Incident, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []postgres.Incident
	for _, inc := range f.incidents {
		if f.status[inc.ID] == "firing" {
			out = append(out, inc)
		}
	}
	return out, nil
}

func (f *fakeIncidents) OpenIncident(_ context.Context, inc postgres.Incident, msg string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.incidents {
		if existing.Fingerprint == inc.Fingerprint && f.status[existing.ID] == "firing" {
			return false, nil
		}
	}
	inc.ID = int64(len(f.incidents) + 1)
	f.incidents = append(f.incidents, inc)
	f.status[inc.ID] = "firing"
	f.events = append(f.events, postgres.Event{ID: int64(len(f.events) + 1), IncidentID: inc.ID, Kind: "opened", Message: msg})
	return true, nil
}

func (f *fakeIncidents) ResolveIncident(_ context.Context, id int64, _ time.Time, reason, msg string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status[id] != "firing" {
		return false, nil
	}
	f.status[id], f.reasons[id] = "resolved", reason
	f.events = append(f.events, postgres.Event{ID: int64(len(f.events) + 1), IncidentID: id, Kind: "resolved", Message: msg})
	return true, nil
}

func (f *fakeIncidents) TouchIncident(_ context.Context, id int64, lastEval time.Time, summary string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.incidents {
		if f.incidents[i].ID == id {
			f.incidents[i].LastEvalAt, f.incidents[i].Summary = lastEval, summary
		}
	}
	return nil
}

func (f *fakeIncidents) PendingEvents(_ context.Context, limit int) ([]postgres.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []postgres.Event
	for _, e := range f.events {
		if !f.notified[e.ID] && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeIncidents) MarkNotified(_ context.Context, ids []int64, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		f.notified[id] = true
	}
	return nil
}

type fakeClaims struct {
	mu   sync.Mutex
	vals map[string]string
}

func newFakeClaims() *fakeClaims { return &fakeClaims{vals: map[string]string{}} }

func (f *fakeClaims) Claim(_ context.Context, key string, _ time.Duration) (redis.Claim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch f.vals[key] {
	case "":
		f.vals[key] = "pending"
		return redis.Claim{Result: redis.Acquired}, nil
	case "sent":
		return redis.Claim{Result: redis.AlreadySent}, nil
	default:
		return redis.Claim{Result: redis.InProgress}, nil
	}
}

func (f *fakeClaims) MarkSent(_ context.Context, key string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.vals[key] = "sent"
	return nil
}

func (f *fakeClaims) Release(_ context.Context, key string, _ redis.Claim) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.vals[key] == "pending" {
		delete(f.vals, key)
	}
	return nil
}

type fakeNotifier struct {
	mu    sync.Mutex
	sent  []string
	fails []error // returned by consecutive calls before succeeding
	calls int
}

func (f *fakeNotifier) Send(_ context.Context, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.fails) > 0 {
		err := f.fails[0]
		f.fails = f.fails[1:]
		return err
	}
	f.sent = append(f.sent, text)
	return nil
}

type harness struct {
	source    *fakeSource
	incidents *fakeIncidents
	claims    *fakeClaims
	notifier  *fakeNotifier
	alerter   *Alerter
}

func newHarness() *harness {
	h := &harness{source: &fakeSource{}, incidents: newFakeIncidents(), claims: newFakeClaims(), notifier: &fakeNotifier{}}
	h.alerter = h.newAlerter()
	return h
}

// newAlerter makes another alerter over the same stores: a second replica.
func (h *harness) newAlerter() *Alerter {
	return New(DefaultConfig, h.source, h.incidents, h.claims, h.notifier, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

var t0 = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func minute(i int) time.Time { return t0.Add(time.Duration(i) * time.Minute) }

func forecast(i int, firing bool, mountpoint string) chstore.Detection {
	eta := 3*time.Hour + 20*time.Minute
	d := chstore.Detection{
		EvalTime: minute(i), Rule: "disk_fill_forecast", Kind: "forecast", Status: "no_trend", Firing: firing,
		Target: "localhost:9100", Name: "node_filesystem_avail_bytes", SeriesID: uint64(len(mountpoint)),
		Labels:   map[string]string{"mountpoint": mountpoint, "device_error": ""},
		Expected: 12 * (1 << 30), Slope: -(1 << 20),
	}
	if firing {
		d.Status, d.ETA = "exhausts", &eta
	}
	return d
}

// tick runs one round "a few seconds after" minute i.
func (h *harness) tick(t *testing.T, a *Alerter, i int) {
	t.Helper()
	if err := a.Tick(t.Context(), minute(i).Add(20*time.Second)); err != nil {
		t.Fatalf("tick at minute %d: %v", i, err)
	}
}

func TestDecide(t *testing.T) {
	h := func(firing ...bool) []chstore.Detection {
		out := make([]chstore.Detection, len(firing))
		for i, f := range firing {
			out[i].Firing = f
		}
		return out
	}
	tests := []struct {
		name    string
		history []chstore.Detection
		want    verdict
	}{
		{"single blip does not open", h(false, false, true), keep},
		{"two in a row open", h(false, true, true), fire},
		{"one quiet minute does not resolve", h(true, true, false), keep},
		{"two quiet minutes do not resolve", h(true, false, false), keep},
		{"three quiet minutes resolve", h(true, false, false, false), clear},
		{"flapping keeps the current state", h(true, false, true, false, true, false), keep},
		{"too little history", h(true), keep},
	}
	for _, tt := range tests {
		if got := decide(tt.history, 2, 3); got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestIncidentLifecycle(t *testing.T) {
	h := newHarness()

	h.source.add(forecast(1, true, "/"))
	h.tick(t, h.alerter, 1)
	if len(h.incidents.incidents) != 0 {
		t.Fatal("a single firing evaluation opened an incident")
	}

	h.source.add(forecast(2, true, "/"))
	h.tick(t, h.alerter, 2)
	if len(h.incidents.incidents) != 1 || len(h.notifier.sent) != 1 {
		t.Fatalf("after two firing minutes: %d incidents, %d messages", len(h.incidents.incidents), len(h.notifier.sent))
	}
	msg := h.notifier.sent[0]
	for _, want := range []string{"🔴 Диск заполняется", "localhost:9100 mountpoint=/", "заполнится через 3 ч 20 мин", "свободно 12.0 ГиБ", "60.0 МиБ/мин"} {
		if !strings.Contains(msg, want) {
			t.Errorf("opened message lacks %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "device_error") {
		t.Errorf("empty labels leak into the message:\n%s", msg)
	}

	// Still firing: no new message, but the incident follows the series.
	h.source.add(forecast(3, true, "/"))
	h.tick(t, h.alerter, 3)
	if len(h.notifier.sent) != 1 || !h.incidents.incidents[0].LastEvalAt.Equal(minute(3)) {
		t.Errorf("ongoing incident: %d messages, last eval %v", len(h.notifier.sent), h.incidents.incidents[0].LastEvalAt)
	}

	for i := 4; i <= 6; i++ {
		h.source.add(forecast(i, false, "/"))
		h.tick(t, h.alerter, i)
	}
	if h.incidents.status[1] != "resolved" || h.incidents.reasons[1] != postgres.ResolveRecovered {
		t.Fatalf("after three quiet minutes: status %s reason %s", h.incidents.status[1], h.incidents.reasons[1])
	}
	if len(h.notifier.sent) != 2 || !strings.Contains(h.notifier.sent[1], "✅ Решено") ||
		!strings.Contains(h.notifier.sent[1], "длился 4 мин") {
		t.Errorf("resolved message: %q", h.notifier.sent)
	}

	// The same minute processed again (alerter restart) changes nothing.
	again := h.newAlerter()
	h.tick(t, again, 6)
	if len(h.incidents.events) != 2 || len(h.notifier.sent) != 2 {
		t.Errorf("reprocessing: %d events, %d messages; want 2 and 2", len(h.incidents.events), len(h.notifier.sent))
	}
}

func TestStaleIncidents(t *testing.T) {
	h := newHarness()
	h.source.add(forecast(1, true, "/data"), forecast(2, true, "/data"), forecast(2, false, "/"))
	h.tick(t, h.alerter, 2)
	if len(h.incidents.incidents) != 1 {
		t.Fatalf("setup: %d incidents", len(h.incidents.incidents))
	}

	// Nothing evaluated at all for a long time: the processor is down.
	// That is not a recovery, so the incident stays open.
	quiet := h.newAlerter()
	if err := quiet.Tick(t.Context(), minute(30)); err != nil {
		t.Fatal(err)
	}
	if h.incidents.status[1] != "firing" {
		t.Fatal("incident resolved while nothing was evaluated at all")
	}

	// Evaluations resume, but /data is no longer among them (unmounted).
	h.source.add(forecast(31, false, "/"))
	h.tick(t, quiet, 31)
	if h.incidents.status[1] != "resolved" || h.incidents.reasons[1] != postgres.ResolveNoData {
		t.Errorf("status %s reason %s, want resolved as no_data", h.incidents.status[1], h.incidents.reasons[1])
	}
	if last := h.notifier.sent[len(h.notifier.sent)-1]; !strings.Contains(last, "перестал оцениваться") {
		t.Errorf("no-data message: %q", last)
	}
}

func TestTransitionsOfOneRoundGoOutAsOneMessage(t *testing.T) {
	h := newHarness()
	mounts := []string{"/", "/System/Volumes/Data", "/System/Volumes/VM", "/System/Volumes/Preboot"}
	for i := 1; i <= 2; i++ {
		for _, m := range mounts {
			h.source.add(forecast(i, true, m))
		}
	}
	h.tick(t, h.alerter, 2)
	if len(h.notifier.sent) != 1 || strings.Count(h.notifier.sent[0], "🔴") != len(mounts) {
		t.Errorf("want one message with %d incidents, got %d messages: %q", len(mounts), len(h.notifier.sent), h.notifier.sent)
	}
}

func TestFailedSendIsRetriedOnce(t *testing.T) {
	h := newHarness()
	h.notifier.fails = []error{errors.New("connection reset")}
	h.source.add(forecast(1, true, "/"), forecast(2, true, "/"))

	if err := h.alerter.Tick(t.Context(), minute(2)); err == nil {
		t.Fatal("tick reported success although the send failed")
	}
	if len(h.notifier.sent) != 0 || h.incidents.notified[1] {
		t.Fatal("event marked delivered after a failed send")
	}

	// The claim was released, so the very next round retries.
	h.tick(t, h.alerter, 2)
	h.tick(t, h.alerter, 2)
	if len(h.notifier.sent) != 1 || !h.incidents.notified[1] {
		t.Errorf("after retry: %d messages, notified %v; want exactly one", len(h.notifier.sent), h.incidents.notified[1])
	}
}

func TestSentButNotRecordedIsNotResent(t *testing.T) {
	h := newHarness()
	h.source.add(forecast(1, true, "/"), forecast(2, true, "/"))
	h.tick(t, h.alerter, 2)

	// Simulate a crash after the send and MarkSent, before the outbox row
	// was updated: the event looks pending again.
	h.incidents.notified[1] = false
	h.tick(t, h.newAlerter(), 2)
	if len(h.notifier.sent) != 1 || !h.incidents.notified[1] {
		t.Errorf("%d messages after the crash scenario, notified %v; want 1 and true", len(h.notifier.sent), h.incidents.notified[1])
	}
}

func TestConcurrentAlertersSendOnce(t *testing.T) {
	h := newHarness()
	h.source.add(forecast(1, true, "/"), forecast(2, true, "/"))

	var wg sync.WaitGroup
	for range 5 {
		a := h.newAlerter()
		wg.Go(func() { _ = a.Tick(t.Context(), minute(2)) })
	}
	wg.Wait()
	// Laggards that saw "pending" retry next round and find "sent".
	h.tick(t, h.newAlerter(), 2)

	if len(h.incidents.incidents) != 1 || len(h.notifier.sent) != 1 {
		t.Errorf("%d incidents, %d messages from 5 concurrent alerters; want 1 and 1",
			len(h.incidents.incidents), len(h.notifier.sent))
	}
}

func TestFloodControlPausesDelivery(t *testing.T) {
	h := newHarness()
	h.notifier.fails = []error{&notify.RetryAfterError{After: 30 * time.Second}}
	h.source.add(forecast(1, true, "/"), forecast(2, true, "/"))

	_ = h.alerter.Tick(t.Context(), minute(2))
	calls := h.notifier.calls
	_ = h.alerter.Tick(t.Context(), minute(2).Add(10*time.Second))
	if h.notifier.calls != calls {
		t.Error("sent again before Telegram's retry_after passed")
	}
	_ = h.alerter.Tick(t.Context(), minute(2).Add(31*time.Second))
	if len(h.notifier.sent) != 1 {
		t.Errorf("after the pause: %d messages, want 1", len(h.notifier.sent))
	}
}

func TestChunks(t *testing.T) {
	ev := func(n int) outgoing { return outgoing{event: postgres.Event{Message: strings.Repeat("я", n)}} }
	got := chunks([]outgoing{ev(40), ev(40), ev(40), ev(500)}, 100)
	if len(got) != 3 || len(got[0]) != 2 || len(got[1]) != 1 || len(got[2]) != 1 {
		t.Fatalf("chunk sizes: %v", got)
	}
	if n := len([]rune(got[2][0].event.Message)); n != 100 || !strings.HasSuffix(got[2][0].event.Message, "…") {
		t.Errorf("oversized event: %d runes", n)
	}
}

func TestFormatting(t *testing.T) {
	durations := map[time.Duration]string{
		20 * time.Second:                               "меньше минуты",
		3*time.Hour + 20*time.Minute:                   "3 ч 20 мин",
		45*time.Hour + 34*time.Minute + 28*time.Second: "1 д 21 ч 34 мин",
		48 * time.Hour:                                 "2 д",
	}
	for d, want := range durations {
		if got := formatDuration(d); got != want {
			t.Errorf("formatDuration(%v) = %q, want %q", d, got, want)
		}
	}
	bytes := map[float64]string{512: "512 Б", 1536: "1.5 КиБ", 12 * (1 << 30): "12.0 ГиБ", -(1 << 20): "-1.0 МиБ"}
	for b, want := range bytes {
		if got := formatBytes(b); got != want {
			t.Errorf("formatBytes(%v) = %q, want %q", b, got, want)
		}
	}
}

func TestGrowthMessage(t *testing.T) {
	d := chstore.Detection{
		Rule: "goroutine_leak", Kind: "growth", Target: "cloute:8080", Name: "go_goroutines",
		Expected: 380, Slope: 3.0 / 60, Score: 41.7, Points: 60,
	}
	msg := openedMessage(d)
	for _, want := range []string{"🔴 Похоже на утечку горутин", "cloute:8080", "растёт на 180 в час, сейчас около 380", "t = 41.7"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message lacks %q:\n%s", want, msg)
		}
	}
}

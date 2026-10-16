package queue

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/te1pe/foresight/internal/logs"
	"github.com/te1pe/foresight/internal/scrape"
)

// Same version as deploy/docker-compose.yml.
const redpandaImage = "redpandadata/redpanda:v26.2.4"

var (
	broker string
	rp     *redpanda.Container
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("skipping redpanda integration tests in -short mode")
		os.Exit(0)
	}
	os.Exit(runWithRedpanda(m))
}

func runWithRedpanda(m *testing.M) int {
	ctx := context.Background()
	var err error
	rp, err = redpanda.Run(ctx, redpandaImage)
	defer func() {
		if err := testcontainers.TerminateContainer(rp); err != nil {
			fmt.Fprintln(os.Stderr, "terminate container:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start redpanda container:", err)
		return 1
	}
	broker, err = rp.KafkaSeedBroker(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "seed broker:", err)
		return 1
	}
	return m.Run()
}

// newTopic creates a uniquely named topic with several partitions, so the
// tests exercise per-partition offsets the way production does.
func newTopic(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	code, out, err := rp.Exec(t.Context(), []string{"rpk", "topic", "create", name, "-p", "3", "-r", "1"})
	if err != nil || code != 0 {
		msg, _ := io.ReadAll(out)
		t.Fatalf("create topic %s: code %d, err %v: %s", name, code, err, msg)
	}
	return name
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func batch(target string, at time.Time) scrape.Batch {
	return scrape.Batch{Target: target, Time: at, Samples: []scrape.Sample{
		{Name: "up", Value: 1, Type: scrape.TypeGauge},
	}}
}

func publish(t *testing.T, topic string, batches ...scrape.Batch) {
	t.Helper()
	p, err := NewProducer(ProducerConfig{Brokers: []string{broker}, SamplesTopic: topic}, testLogger(t))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	in := make(chan scrape.Batch, len(batches))
	for _, b := range batches {
		in <- b
	}
	close(in)
	p.Forward(t.Context(), in)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// startConsumer runs a consumer and returns a function that stops it the
// way shutdown does, including leaving the group.
func startConsumer(t *testing.T, topic, group string, handle Handler[scrape.Batch], permanent func(error) bool) (stop func()) {
	t.Helper()
	c, err := NewConsumer(ConsumerConfig[scrape.Batch]{
		Brokers: []string{broker}, Topic: topic, Group: group, Decode: Decode, Handle: handle, Permanent: permanent,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.minBackoff, c.maxBackoff = 10*time.Millisecond, 50*time.Millisecond

	pollCtx, stopPolling := context.WithCancel(t.Context())
	workCtx, cancelWork := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(pollCtx, workCtx)
	}()

	var once atomic.Bool
	stop = func() {
		if !once.CompareAndSwap(false, true) {
			return
		}
		// Mirrors cmd/processor: stop polling, let the batch in hand be
		// committed, and cancel the work only after a grace period. A
		// consumer stuck retrying a failing handler is cut off by it.
		stopPolling()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			cancelWork()
			<-done
		}
		cancelWork()
		c.Close(context.Background())
	}
	t.Cleanup(stop)
	return stop
}

// collector is a handler that records every batch it successfully handles.
func collector() (Handler[scrape.Batch], <-chan scrape.Batch) {
	ch := make(chan scrape.Batch, 100)
	return func(_ context.Context, batches []scrape.Batch) error {
		for _, b := range batches {
			ch <- b
		}
		return nil
	}, ch
}

func receive(t *testing.T, ch <-chan scrape.Batch, n int) []scrape.Batch {
	t.Helper()
	var got []scrape.Batch
	timeout := time.After(30 * time.Second)
	for len(got) < n {
		select {
		case b := <-ch:
			got = append(got, b)
		case <-timeout:
			t.Fatalf("received %d of %d batches", len(got), n)
		}
	}
	return got
}

// expectOnly asserts that a fresh consumer in group sees only the marker,
// i.e. everything before it was committed.
func expectOnly(t *testing.T, topic, group, marker string) {
	t.Helper()
	publish(t, topic, batch(marker, time.Now()))
	handle, ch := collector()
	stop := startConsumer(t, topic, group, handle, nil)
	defer stop()

	got := receive(t, ch, 1)
	if got[0].Target != marker {
		t.Fatalf("got redelivered batch %q, want only the marker %q", got[0].Target, marker)
	}
	// Give stray redeliveries a moment to show up.
	select {
	case b := <-ch:
		t.Fatalf("unexpected batch %q after the marker", b.Target)
	case <-time.After(time.Second):
	}
}

func TestProduceConsumeRoundTrip(t *testing.T) {
	topic := newTopic(t)
	at := time.Date(2026, 10, 15, 12, 0, 0, 123456789, time.UTC)
	want := map[string]bool{}
	var batches []scrape.Batch
	for i := range 6 {
		target := fmt.Sprintf("host-%d:9100", i)
		want[target] = true
		batches = append(batches, batch(target, at))
	}
	publish(t, topic, batches...)

	handle, ch := collector()
	startConsumer(t, topic, "g", handle, nil)

	for _, b := range receive(t, ch, len(batches)) {
		if !want[b.Target] {
			t.Errorf("unexpected or duplicate target %q", b.Target)
		}
		delete(want, b.Target)
		if !b.Time.Equal(at) || len(b.Samples) != 1 || b.Samples[0].Name != "up" {
			t.Errorf("batch %s arrived changed: %+v", b.Target, b)
		}
	}
}

func TestUncommittedRecordsAreRedelivered(t *testing.T) {
	topic := newTopic(t)
	publish(t, topic, batch("first", time.Now()))

	// The first consumer never manages to store the batch, as if the
	// database were down, and is shut down while still retrying.
	var attempts atomic.Int32
	failing := func(context.Context, []scrape.Batch) error {
		attempts.Add(1)
		return errors.New("database unavailable")
	}
	stop := startConsumer(t, topic, "g", failing, nil)
	deadline := time.Now().Add(30 * time.Second)
	for attempts.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if attempts.Load() < 3 {
		t.Fatalf("handler called %d times, want retries", attempts.Load())
	}
	stop()

	// Nothing was committed, so the same group gets the batch again.
	handle, ch := collector()
	stop = startConsumer(t, topic, "g", handle, nil)
	if got := receive(t, ch, 1); got[0].Target != "first" {
		t.Fatalf("got %q, want the redelivered batch", got[0].Target)
	}
	stop()

	// Now it was committed and must not come back.
	expectOnly(t, topic, "g", "marker")
}

var errPoison = errors.New("poison")

func TestPermanentErrorDropsOnlyTheBadBatch(t *testing.T) {
	topic := newTopic(t)
	now := time.Now()
	// The same key puts all three on one partition, so they are most likely
	// handled in one call, which is the case that needs narrowing down.
	good1, bad, good2 := batch("same", now), batch("same", now.Add(time.Second)), batch("same", now.Add(2*time.Second))
	bad.Samples[0].Name = "poison"
	publish(t, topic, good1, bad, good2)

	stored := make(chan scrape.Batch, 10)
	handle := func(_ context.Context, batches []scrape.Batch) error {
		for _, b := range batches {
			if b.Samples[0].Name == "poison" {
				return fmt.Errorf("insert: %w", errPoison)
			}
		}
		for _, b := range batches {
			stored <- b
		}
		return nil
	}
	isPoison := func(err error) bool { return errors.Is(err, errPoison) }
	stop := startConsumer(t, topic, "g", handle, isPoison)

	got := receive(t, stored, 2)
	if !got[0].Time.Equal(good1.Time) || !got[1].Time.Equal(good2.Time) {
		t.Errorf("stored %v and %v, want both good batches in order", got[0].Time, got[1].Time)
	}
	stop()

	// The poison batch was dropped and committed past, not retried forever.
	expectOnly(t, topic, "g", "marker")
}

func TestUndecodableRecordIsSkipped(t *testing.T) {
	topic := newTopic(t)

	raw, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer raw.Close()
	if err := raw.ProduceSync(t.Context(), &kgo.Record{Key: []byte("k"), Value: []byte("not a batch")}).FirstErr(); err != nil {
		t.Fatalf("produce garbage: %v", err)
	}
	publish(t, topic, batch("valid", time.Now()))

	handle, ch := collector()
	stop := startConsumer(t, topic, "g", handle, nil)
	if got := receive(t, ch, 1); got[0].Target != "valid" {
		t.Fatalf("got %q, want the valid batch", got[0].Target)
	}
	stop()

	expectOnly(t, topic, "g", "marker")
}

func TestLogsPublishAndConsume(t *testing.T) {
	topic := newTopic(t)
	p, err := NewProducer(ProducerConfig{Brokers: []string{broker}, LogsTopic: topic}, testLogger(t))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	at := time.Date(2026, 10, 16, 12, 0, 0, 123456789, time.UTC)
	sent := logs.Batch{Service: "cloute", Received: at, Entries: []logs.Entry{
		{Time: at, Level: "error", Message: "apply op", Attrs: map[string]string{"doc": "d1"}, Raw: `{"msg":"apply op"}`},
	}}
	// Returns only after the broker acknowledged the record.
	if err := p.PublishLogs(t.Context(), sent); err != nil {
		t.Fatalf("PublishLogs: %v", err)
	}

	got := make(chan logs.Batch, 1)
	c, err := NewConsumer(ConsumerConfig[logs.Batch]{
		Brokers: []string{broker}, Topic: topic, Group: "g", Decode: DecodeLogs,
		Handle: func(_ context.Context, bs []logs.Batch) error {
			for _, b := range bs {
				got <- b
			}
			return nil
		},
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	pollCtx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { c.Run(pollCtx, t.Context()); close(done) }()
	defer func() { stop(); <-done; c.Close(context.Background()) }()

	select {
	case b := <-got:
		e := b.Entries[0]
		if b.Service != "cloute" || !b.Received.Equal(at) || len(b.Entries) != 1 ||
			!e.Time.Equal(at) || e.Level != "error" || e.Message != "apply op" || e.Attrs["doc"] != "d1" || e.Raw != `{"msg":"apply op"}` {
			t.Errorf("round trip changed the batch: %+v", b)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("log batch not consumed")
	}
}

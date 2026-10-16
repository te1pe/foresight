// Package loadgen drives foresight with synthetic targets and pushed logs
// and reports throughput, latency and resource use from foresight's own
// metrics.
package loadgen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type Config struct {
	Targets       int           // synthetic targets to register
	Duration      time.Duration // measurement window
	Warmup        time.Duration // run time before measuring starts
	LogsPerSecond int           // pushed log lines per second; 0 disables
	ExporterAddr  string        // where the fake exporter listens
	Fixture       string        // exposition served by every target
	IngestURL     string        // ingest public API, for POST /v1/logs
	IngestAdmin   string        // ingest /metrics
	ProcessorAdm  string        // processor /metrics
}

func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := Config{
		Targets:       100,
		Duration:      2 * time.Minute,
		Warmup:        20 * time.Second,
		ExporterAddr:  "127.0.0.1:9300",
		Fixture:       "testdata/node_exporter.txt",
		IngestURL:     "http://127.0.0.1:8080",
		IngestAdmin:   "http://127.0.0.1:6060/metrics",
		ProcessorAdm:  "http://127.0.0.1:6061/metrics",
		LogsPerSecond: 0,
	}
	var err error
	if v := getenv("FORESIGHT_LOADGEN_TARGETS"); v != "" {
		if cfg.Targets, err = strconv.Atoi(v); err != nil || cfg.Targets <= 0 {
			return cfg, fmt.Errorf("FORESIGHT_LOADGEN_TARGETS: want a positive integer, got %q", v)
		}
	}
	if v := getenv("FORESIGHT_LOADGEN_LOGS_RPS"); v != "" {
		if cfg.LogsPerSecond, err = strconv.Atoi(v); err != nil || cfg.LogsPerSecond < 0 {
			return cfg, fmt.Errorf("FORESIGHT_LOADGEN_LOGS_RPS: want a non-negative integer, got %q", v)
		}
	}
	for name, dst := range map[string]*time.Duration{
		"FORESIGHT_LOADGEN_DURATION": &cfg.Duration, "FORESIGHT_LOADGEN_WARMUP": &cfg.Warmup,
	} {
		if v := getenv(name); v != "" {
			if *dst, err = time.ParseDuration(v); err != nil || *dst <= 0 {
				return cfg, fmt.Errorf("%s: want a positive duration, got %q", name, v)
			}
		}
	}
	return cfg, nil
}

// TargetStore is where the synthetic targets are registered: the same
// targets table ingest reads, so the load goes through the real path.
type TargetStore interface {
	AddTargets(ctx context.Context, targets []string) error
	RemoveTargets(ctx context.Context, targets []string) error
}

func targetNames(cfg Config) []string {
	names := make([]string, cfg.Targets)
	for i := range names {
		names[i] = fmt.Sprintf("http://%s/t/%d/metrics", cfg.ExporterAddr, i)
	}
	return names
}

// Run registers the targets, waits until ingest scrapes all of them, lets
// the pipeline warm up, measures for cfg.Duration and removes the targets
// again, also on cancellation.
func Run(ctx context.Context, cfg Config, store TargetStore, log *slog.Logger) (Report, error) {
	fixture, err := os.ReadFile(cfg.Fixture)
	if err != nil {
		return Report{}, fmt.Errorf("read fixture: %w", err)
	}

	stopExporter, err := serveExporter(cfg.ExporterAddr, fixture, log)
	if err != nil {
		return Report{}, err
	}
	defer stopExporter()

	names := targetNames(cfg)
	if err := store.AddTargets(ctx, names); err != nil {
		return Report{}, err
	}
	defer func() {
		// Fresh context: cleanup must happen even when ctx was cancelled.
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.RemoveTargets(cctx, names); err != nil {
			log.Error("remove synthetic targets", "err", err)
		}
		log.Info("synthetic targets removed", "targets", len(names))
	}()
	log.Info("synthetic targets registered; waiting for ingest to pick them up", "targets", len(names))

	client := &http.Client{Timeout: 10 * time.Second}
	if err := waitForTargets(ctx, client, cfg.IngestAdmin, float64(cfg.Targets)); err != nil {
		return Report{}, err
	}

	var pusher *logPusher
	if cfg.LogsPerSecond > 0 {
		pusher = &logPusher{url: cfg.IngestURL + "/v1/logs?service=loadgen", perSecond: cfg.LogsPerSecond, client: client}
		pctx, stopPush := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); pusher.run(pctx) }()
		defer func() { stopPush(); <-done }()
	}

	log.Info("all targets scraped; warming up", "warmup", cfg.Warmup.String())
	if err := sleep(ctx, cfg.Warmup); err != nil {
		return Report{}, err
	}

	before, err := readAll(ctx, client, cfg)
	if err != nil {
		return Report{}, err
	}
	var logsBefore logCounts
	if pusher != nil {
		logsBefore = pusher.counts()
	}
	log.Info("measuring", "duration", cfg.Duration.String())
	if err := sleep(ctx, cfg.Duration); err != nil {
		return Report{}, err
	}
	after, err := readAll(ctx, client, cfg)
	if err != nil {
		return Report{}, err
	}

	r := buildReport(cfg, before, after)
	if pusher != nil {
		c := pusher.counts()
		r.LogsSent = c.sent - logsBefore.sent
		r.LogsFailed = c.failed - logsBefore.failed
	}
	return r, nil
}

type readings struct{ ingest, processor snapshot }

func readAll(ctx context.Context, client *http.Client, cfg Config) (readings, error) {
	ing, err := fetchSnapshot(ctx, client, cfg.IngestAdmin)
	if err != nil {
		return readings{}, err
	}
	proc, err := fetchSnapshot(ctx, client, cfg.ProcessorAdm)
	if err != nil {
		return readings{}, err
	}
	return readings{ingest: ing, processor: proc}, nil
}

// waitForTargets polls ingest's own gauge until it scrapes at least want
// targets: the targets table is re-read every 30 seconds.
func waitForTargets(ctx context.Context, client *http.Client, url string, want float64) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		s, err := fetchSnapshot(ctx, client, url)
		if err == nil && s.sum("foresight_scrape_targets", nil) >= want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ingest did not pick up %v targets within 2 minutes (last error: %v)", want, err)
		}
		if err := sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// serveExporter answers GET /t/{i}/metrics with the fixture for any i: one
// listener stands in for any number of node_exporters.
func serveExporter(addr string, fixture []byte, log *slog.Logger) (stop func(), err error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /t/{i}/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write(fixture)
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("fake exporter: %w", err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("fake exporter", "err", err)
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		<-done
	}, nil
}

type logCounts struct{ sent, failed int64 }

// logPusher posts batches ten times a second. Every line carries its own
// timestamp and sequence number, so deduplication collapses nothing and
// the stored count can be compared with the sent count.
type logPusher struct {
	url       string
	perSecond int
	client    *http.Client
	seq       int64 // touched only by run's goroutine
	sent      atomic.Int64
	failed    atomic.Int64
}

func (p *logPusher) counts() logCounts { return logCounts{p.sent.Load(), p.failed.Load()} }

func (p *logPusher) run(ctx context.Context) {
	const ticksPerSecond = 10
	perTick := max(p.perSecond/ticksPerSecond, 1)
	ticker := time.NewTicker(time.Second / ticksPerSecond)
	defer ticker.Stop()
	var body bytes.Buffer
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		body.Reset()
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for range perTick {
			p.seq++
			fmt.Fprintf(&body, `{"time":%q,"level":"INFO","msg":"loadgen line","seq":%d,"doc":"d%d"}`+"\n", now, p.seq, p.seq%100)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body.Bytes()))
		if err != nil {
			return
		}
		resp, err := p.client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		if err != nil || resp.StatusCode != http.StatusAccepted {
			p.failed.Add(int64(perTick))
			continue
		}
		p.sent.Add(int64(perTick))
	}
}

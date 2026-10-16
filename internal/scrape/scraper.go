package scrape

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// Batch is the result of one scrape of one target. Every sample shares Time
// (the moment the scrape started), so all series of a scrape line up exactly
// on the time axis — the property later window aggregation relies on.
type Batch struct {
	Target  string
	Time    time.Time
	Samples []Sample
}

// TargetStatus is the in-memory view of a target served by /v1/targets.
type TargetStatus struct {
	Target      string
	LastScrape  time.Time // zero until the first scrape completes
	Up          bool
	Samples     int
	Duration    time.Duration
	ParseErrors int
	LastError   string // kept after recovery, so a flapping target is visible
	LastErrorAt time.Time
}

type Config struct {
	Interval time.Duration
	Timeout  time.Duration
}

type Scraper struct {
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	out      chan<- Batch
	log      *slog.Logger

	mu     sync.Mutex
	status map[string]TargetStatus
}

type target struct {
	name string // as listed in the targets table; stored in samples.target
	url  string
}

// New validates the configuration. Batches are sent to out; the caller owns
// the channel and closes it after Run returns. The set of targets is not
// fixed: Run receives it, and its changes, over a channel.
func New(cfg Config, out chan<- Batch, log *slog.Logger) (*Scraper, error) {
	if cfg.Interval <= 0 || cfg.Timeout <= 0 {
		return nil, fmt.Errorf("interval %s and timeout %s must be positive", cfg.Interval, cfg.Timeout)
	}

	s := &Scraper{
		interval: cfg.Interval,
		timeout:  cfg.Timeout,
		// The per-request deadline comes from the context; the client itself
		// has no timeout so the two cannot disagree.
		client: &http.Client{Transport: scrapeTransport()},
		out:    out,
		log:    log,
		status: make(map[string]TargetStatus),
	}
	return s, nil
}

// scrapeTransport keeps connections to targets alive between scrapes. The
// default keeps only 2 idle connections per host:port, so several exporters
// behind one address (or the load test's fake exporter) reconnected on
// every scrape: in the first load run that showed as a 3 s scrape p99 and
// sockets piling up in TIME_WAIT.
func scrapeTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 1024
	t.MaxIdleConnsPerHost = 256
	return t
}

// targetURL lets a target be a bare "host:port", the way Prometheus
// static_configs do, while still accepting full URLs.
func targetURL(name string) (string, error) {
	raw := name
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", errors.New("missing host")
	}
	if u.Path == "" {
		u.Path = "/metrics"
	}
	return u.String(), nil
}

// running is one target's scrape goroutine and the means to stop it.
type running struct {
	url    string
	cancel context.CancelFunc
	done   chan struct{}
}

// Run keeps one goroutine per target, reconciling the running set with
// every list received on updates: new targets start, removed ones stop,
// unchanged ones keep their rhythm. It returns only after every goroutine
// has exited, so the caller can safely close the output channel afterwards.
func (s *Scraper) Run(ctx context.Context, updates <-chan []string) {
	active := map[string]*running{}
	defer func() {
		for name, r := range active {
			s.stop(name, r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case names, ok := <-updates:
			if !ok {
				// No more updates: keep scraping the current set until ctx ends.
				updates = nil
				continue
			}
			s.reconcile(ctx, names, active)
		}
	}
}

func (s *Scraper) reconcile(ctx context.Context, names []string, active map[string]*running) {
	desired := make(map[string]string, len(names))
	for _, name := range names {
		u, err := targetURL(name)
		if err != nil {
			// One bad row in the targets table must not stop the others.
			s.log.Warn("skip invalid target", "target", name, "err", err)
			continue
		}
		desired[name] = u
	}

	for name, r := range active {
		// A changed URL for the same name is a restart: stop, then start.
		if u, keep := desired[name]; !keep || u != r.url {
			s.stop(name, r)
			delete(active, name)
		}
	}
	for name, u := range desired {
		if _, ok := active[name]; ok {
			continue
		}
		active[name] = s.start(ctx, target{name: name, url: u})
	}
}

func (s *Scraper) start(ctx context.Context, t target) *running {
	s.mu.Lock()
	s.status[t.name] = TargetStatus{Target: t.name}
	scrapeTargets.Set(float64(len(s.status)))
	s.mu.Unlock()

	tctx, cancel := context.WithCancel(ctx)
	r := &running{url: t.url, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		s.loop(tctx, t)
	}()
	s.log.Info("target added", "target", t.name, "url", t.url)
	return r
}

// stop cancels the target's goroutine and waits for it: a scrape in flight
// is aborted, and a batch it was about to send is dropped rather than
// delivered for a target that is no longer configured.
func (s *Scraper) stop(name string, r *running) {
	r.cancel()
	<-r.done

	s.mu.Lock()
	delete(s.status, name)
	scrapeTargets.Set(float64(len(s.status)))
	s.mu.Unlock()
	forgetTargetMetrics(name)
	s.log.Info("target removed", "target", name)
}

// scrapeOffset spreads targets over the interval, the way Prometheus
// does: without it every target added in one reconcile is scraped in the
// same instant, and the sequential writer downstream gets a burst of
// inserts every interval. The load test measured that: 100 targets at 15s,
// insert p99 140 ms, yet end-to-end p99 11.6 s, all of it queueing behind
// the burst. Hashing the name keeps a target's phase stable across
// restarts, so its samples stay evenly spaced in time.
func scrapeOffset(name string, interval time.Duration) time.Duration {
	h := fnv.New64a()
	h.Write([]byte(name))
	return time.Duration(h.Sum64() % uint64(interval))
}

func (s *Scraper) loop(ctx context.Context, t target) {
	// Wait for the target's phase within the interval before the first
	// scrape; the ticker then keeps that phase.
	first := time.NewTimer(scrapeOffset(t.name, s.interval))
	defer first.Stop()
	select {
	case <-first.C:
	case <-ctx.Done():
		return
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		b, ok := s.scrapeOnce(ctx, t)
		if !ok {
			return
		}
		select {
		case s.out <- b:
		case <-ctx.Done():
			return
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// scrapeOnce never fails the caller: a broken target becomes up=0 plus a log
// line. ok is false only when ctx was cancelled, because a scrape aborted by
// shutdown says nothing about the target's health and must not record up=0.
func (s *Scraper) scrapeOnce(ctx context.Context, t target) (b Batch, ok bool) {
	start := time.Now()
	res, err := s.fetch(ctx, t)
	duration := time.Since(start)
	if ctx.Err() != nil {
		return Batch{}, false
	}

	up := 1.0
	if err != nil {
		up = 0
		s.log.Warn("scrape failed", "target", t.name, "duration", duration.String(), "err", err)
	} else if res.Skipped > 0 {
		s.log.Warn("skipped malformed lines", "target", t.name, "skipped", res.Skipped, "first_err", res.FirstErr)
	}

	scrapeDuration.WithLabelValues(t.name).Observe(duration.Seconds())
	scrapeSamples.WithLabelValues(t.name).Add(float64(len(res.Samples)))
	scrapeParseErrors.WithLabelValues(t.name).Add(float64(res.Skipped))
	if err != nil {
		scrapeFailures.WithLabelValues(t.name).Inc()
	}

	samples := append(res.Samples,
		Sample{Name: "up", Value: up, Type: TypeGauge},
		Sample{Name: "scrape_duration_seconds", Value: duration.Seconds(), Type: TypeGauge},
	)
	s.recordStatus(t.name, start, duration, len(samples), res.Skipped, err)
	return Batch{Target: t.name, Time: start, Samples: samples}, true
}

func (s *Scraper) fetch(ctx context.Context, t target) (ParseResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.url, nil)
	if err != nil {
		return ParseResult{}, fmt.Errorf("scrape %s: %w", t.name, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return ParseResult{}, fmt.Errorf("scrape %s: %w", t.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ParseResult{}, fmt.Errorf("scrape %s: unexpected status %s", t.name, resp.Status)
	}
	// The body is parsed while it streams in, so the deadline also covers a
	// target that sends headers quickly and then stalls.
	res, err := Parse(resp.Body)
	if err != nil {
		return ParseResult{}, fmt.Errorf("scrape %s: %w", t.name, err)
	}
	return res, nil
}

func (s *Scraper) recordStatus(name string, at time.Time, d time.Duration, samples, skipped int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.status[name]
	st.LastScrape = at
	st.Up = err == nil
	st.Samples = samples
	st.Duration = d
	st.ParseErrors = skipped
	if err != nil {
		st.LastError = err.Error()
		st.LastErrorAt = at
	}
	s.status[name] = st
}

// Targets returns a snapshot sorted by target name.
func (s *Scraper) Targets() []TargetStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]TargetStatus, 0, len(s.status))
	for _, st := range s.status {
		out = append(out, st)
	}
	slices.SortFunc(out, func(a, b TargetStatus) int { return strings.Compare(a.Target, b.Target) })
	return out
}

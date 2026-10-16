// Package alert turns detections into incidents and incidents into
// notifications.
//
// The pipeline is split in two steps with a durable hand-off between them:
//
//  1. lifecycle: read recent detections, open or resolve incidents in
//     PostgreSQL; each transition writes its notification into an outbox
//     table in the same transaction;
//  2. delivery: send undelivered outbox events, using Redis claims so that
//     retries and concurrent alerters do not send the same event twice.
//
// A crash between the steps loses nothing: the outbox row stays undelivered
// until the next round picks it up.
package alert

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/te1pe/foresight/internal/notify"
	chstore "github.com/te1pe/foresight/internal/storage/clickhouse"
	"github.com/te1pe/foresight/internal/storage/postgres"
	"github.com/te1pe/foresight/internal/storage/redis"
)

// The four dependencies are interfaces so the orchestration can be tested
// with fakes; each has one real implementation plus the test double
// (Notifier has two real ones: Telegram and the log).
type (
	DetectionSource interface {
		RecentDetections(ctx context.Context, since time.Time) ([]chstore.Detection, error)
	}
	IncidentStore interface {
		OpenIncidents(ctx context.Context) ([]postgres.Incident, error)
		OpenIncident(ctx context.Context, inc postgres.Incident, message string) (bool, error)
		ResolveIncident(ctx context.Context, id int64, at time.Time, reason, message string) (bool, error)
		TouchIncident(ctx context.Context, id int64, lastEval time.Time, summary string) error
		PendingEvents(ctx context.Context, limit int) ([]postgres.Event, error)
		MarkNotified(ctx context.Context, ids []int64, at time.Time) error
	}
	Claimer interface {
		Claim(ctx context.Context, key string, pendingTTL time.Duration) (redis.Claim, error)
		MarkSent(ctx context.Context, key string, ttl time.Duration) error
		Release(ctx context.Context, key string, claim redis.Claim) error
	}
	Notifier interface {
		Send(ctx context.Context, text string) error
	}
)

type Config struct {
	// OpenAfter consecutive firing evaluations open an incident, and
	// ResolveAfter consecutive quiet ones resolve it. The gap between the
	// two is hysteresis: a series hovering at the threshold stays in its
	// current state instead of opening and resolving every other minute.
	OpenAfter    int
	ResolveAfter int
	// StaleAfter resolves an open incident whose series has not been
	// evaluated for that long while other series still are (an unmounted
	// disk, a removed target).
	StaleAfter   time.Duration
	PollInterval time.Duration
}

var DefaultConfig = Config{OpenAfter: 2, ResolveAfter: 3, StaleAfter: 10 * time.Minute, PollInterval: 15 * time.Second}

const (
	maxEventsPerRound = 50
	// pendingClaimTTL bounds how long a crashed sender blocks a retry.
	pendingClaimTTL = 2 * time.Minute
	// sentClaimTTL only has to outlive the retries of one event; a week
	// covers any realistic outage of the alerter.
	sentClaimTTL = 7 * 24 * time.Hour
	// messageBudget leaves headroom under Telegram's limit: it counts UTF-16
	// code units, and emoji take two.
	messageBudget = notify.MaxMessageLen - 200
)

type Alerter struct {
	cfg       Config
	source    DetectionSource
	incidents IncidentStore
	claims    Claimer
	notifier  Notifier
	log       *slog.Logger

	lastEval    time.Time // newest evaluation already applied
	pausedUntil time.Time // Telegram flood control
}

func New(cfg Config, source DetectionSource, incidents IncidentStore, claims Claimer, notifier Notifier, log *slog.Logger) *Alerter {
	return &Alerter{cfg: cfg, source: source, incidents: incidents, claims: claims, notifier: notifier, log: log}
}

// Run ticks until stopCtx is cancelled. The round in progress finishes under
// workCtx, which the caller cancels only when its shutdown deadline expires;
// anything left undone is redone by the next start, because both steps
// work from durable state.
func (a *Alerter) Run(stopCtx, workCtx context.Context) {
	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := a.Tick(workCtx, time.Now()); err != nil {
			a.log.Error("alerter round", "err", err)
		}
		select {
		case <-stopCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *Alerter) Tick(ctx context.Context, now time.Time) error {
	// Delivery runs even if the lifecycle step failed: events queued by an
	// earlier round must not wait for ClickHouse to come back.
	lifecycleErr := a.applyLifecycle(ctx, now)
	deliveryErr := a.deliver(ctx, now)
	err := errors.Join(lifecycleErr, deliveryErr)
	if err != nil {
		roundErrors.Inc()
	}
	return err
}

type verdict int

const (
	keep verdict = iota
	fire
	clear
)

// decide looks at one series' evaluations, oldest first.
func decide(history []chstore.Detection, openAfter, resolveAfter int) verdict {
	tailAll := func(n int, firing bool) bool {
		if len(history) < n {
			return false
		}
		for _, d := range history[len(history)-n:] {
			if d.Firing != firing {
				return false
			}
		}
		return true
	}
	switch {
	case tailAll(openAfter, true):
		return fire
	case tailAll(resolveAfter, false):
		return clear
	default:
		return keep
	}
}

func fingerprint(d chstore.Detection) string {
	return d.Rule + "|" + d.Target + "|" + strconv.FormatUint(d.SeriesID, 10)
}

func (a *Alerter) applyLifecycle(ctx context.Context, now time.Time) error {
	rows, err := a.source.RecentDetections(ctx, now.Add(-a.cfg.StaleAfter))
	if err != nil {
		return fmt.Errorf("read detections: %w", err)
	}
	if len(rows) == 0 {
		// Nothing is being evaluated at all: the processor is down, not
		// every disk recovered. Resolving everything as "no data" here
		// would announce a recovery nobody observed.
		a.log.Warn("no recent detections; incident lifecycle paused")
		return nil
	}

	histories := map[string][]chstore.Detection{}
	var latest time.Time
	for _, d := range rows {
		fp := fingerprint(d)
		histories[fp] = append(histories[fp], d)
		if d.EvalTime.After(latest) {
			latest = d.EvalTime
		}
	}
	if !latest.After(a.lastEval) {
		return nil // no new evaluation round since the last tick
	}

	open, err := a.incidents.OpenIncidents(ctx)
	if err != nil {
		return fmt.Errorf("read open incidents: %w", err)
	}
	openIncidents.Set(float64(len(open)))
	openByFP := make(map[string]postgres.Incident, len(open))
	for _, inc := range open {
		openByFP[inc.Fingerprint] = inc
	}

	var errs []error
	for fp, history := range histories {
		last := history[len(history)-1]
		inc, isOpen := openByFP[fp]
		switch v := decide(history, a.cfg.OpenAfter, a.cfg.ResolveAfter); {
		case v == fire && !isOpen && last.EvalTime.Equal(latest):
			// Only series evaluated in this round may open an incident: one
			// that vanished minutes ago would be resolved as stale at once.
			errs = append(errs, a.open(ctx, fp, last))
		case v == clear && isOpen:
			errs = append(errs, a.resolve(ctx, inc, postgres.ResolveRecovered, last.EvalTime))
		case isOpen && last.EvalTime.After(inc.LastEvalAt):
			errs = append(errs, a.incidents.TouchIncident(ctx, inc.ID, last.EvalTime, summary(last)))
		}
	}
	for fp, inc := range openByFP {
		if _, evaluated := histories[fp]; !evaluated {
			errs = append(errs, a.resolve(ctx, inc, postgres.ResolveNoData, now))
		}
	}

	if err := errors.Join(errs...); err != nil {
		// Leave lastEval behind so the whole round is retried; every
		// transition is idempotent, so the ones that succeeded are no-ops.
		return err
	}
	a.lastEval = latest
	return nil
}

func (a *Alerter) open(ctx context.Context, fp string, d chstore.Detection) error {
	opened, err := a.incidents.OpenIncident(ctx, postgres.Incident{
		Fingerprint: fp, Rule: d.Rule, Target: d.Target, SeriesID: strconv.FormatUint(d.SeriesID, 10),
		Labels: d.Labels, Summary: summary(d), OpenedAt: d.EvalTime, LastEvalAt: d.EvalTime,
	}, openedMessage(d))
	if err == nil && opened {
		transitions.WithLabelValues("opened").Inc()
		a.log.Info("incident opened", "fingerprint", fp, "summary", summary(d))
	}
	return err
}

func (a *Alerter) resolve(ctx context.Context, inc postgres.Incident, reason string, at time.Time) error {
	resolved, err := a.incidents.ResolveIncident(ctx, inc.ID, at, reason, resolvedMessage(inc, reason, at))
	if err == nil && resolved {
		transitions.WithLabelValues("resolved").Inc()
		a.log.Info("incident resolved", "fingerprint", inc.Fingerprint, "reason", reason, "duration", at.Sub(inc.OpenedAt).String())
	}
	return err
}

type outgoing struct {
	event postgres.Event
	key   string
	claim redis.Claim
}

func (a *Alerter) deliver(ctx context.Context, now time.Time) error {
	if now.Before(a.pausedUntil) {
		return nil
	}
	events, err := a.incidents.PendingEvents(ctx, maxEventsPerRound)
	if err != nil {
		return fmt.Errorf("read pending events: %w", err)
	}

	var (
		toSend      []outgoing
		alreadySent []int64
		errs        []error
	)
	for _, e := range events {
		key := "foresight:notify:event:" + strconv.FormatInt(e.ID, 10)
		claim, err := a.claims.Claim(ctx, key, pendingClaimTTL)
		if err != nil {
			// Without Redis there is no duplicate protection, so nothing is
			// sent; the outbox keeps the events until Redis is back.
			return fmt.Errorf("claim events: %w", err)
		}
		switch claim.Result {
		case redis.Acquired:
			toSend = append(toSend, outgoing{event: e, key: key, claim: claim})
		case redis.AlreadySent:
			// Sent before, but the process died before recording it.
			alreadySent = append(alreadySent, e.ID)
			notifications.WithLabelValues("already_sent").Inc()
		case redis.InProgress:
			// Another alerter is sending it right now.
		}
	}
	if err := a.incidents.MarkNotified(ctx, alreadySent, now); err != nil {
		errs = append(errs, err)
	}

	// Several transitions in one round become one message: a full APFS
	// container has a dozen mountpoints with the same free space, and a
	// dozen identical messages would bury the one that matters.
	parts := chunks(toSend, messageBudget)
	for i, chunk := range parts {
		err := a.sendChunk(ctx, chunk, now)
		if err == nil {
			continue
		}
		errs = append(errs, err)
		var flood *notify.RetryAfterError
		if errors.As(err, &flood) {
			a.pausedUntil = now.Add(flood.After)
		}
		// Stop at the first failure (the chat or the network is the
		// problem, not the message) and release the claims of the chunks
		// not attempted, so they are retried next round rather than after
		// the pending TTL.
		for _, rest := range parts[i+1:] {
			a.release(ctx, rest)
		}
		break
	}
	return errors.Join(errs...)
}

func (a *Alerter) release(ctx context.Context, chunk []outgoing) {
	for _, o := range chunk {
		if err := a.claims.Release(ctx, o.key, o.claim); err != nil {
			a.log.Warn("release claim", "key", o.key, "err", err)
		}
	}
}

func (a *Alerter) sendChunk(ctx context.Context, chunk []outgoing, now time.Time) error {
	texts := make([]string, len(chunk))
	for i, o := range chunk {
		texts[i] = o.event.Message
	}
	if err := a.notifier.Send(ctx, strings.Join(texts, "\n\n")); err != nil {
		notifications.WithLabelValues("failed").Add(float64(len(chunk)))
		a.release(ctx, chunk)
		return fmt.Errorf("send %d events: %w", len(chunk), err)
	}
	notifications.WithLabelValues("sent").Add(float64(len(chunk)))

	ids := make([]int64, len(chunk))
	var errs []error
	for i, o := range chunk {
		ids[i] = o.event.ID
		// Recorded in Redis first: if marking the outbox fails, the next
		// round sees "sent" and only fixes the bookkeeping.
		if err := a.claims.MarkSent(ctx, o.key, sentClaimTTL); err != nil {
			errs = append(errs, err)
		}
	}
	if err := a.incidents.MarkNotified(ctx, ids, now); err != nil {
		errs = append(errs, err)
	}
	a.log.Info("notification sent", "events", len(chunk))
	return errors.Join(errs...)
}

// chunks packs events into messages under the length budget. An event
// longer than the budget on its own is truncated rather than dropped.
func chunks(events []outgoing, budget int) [][]outgoing {
	var (
		out  [][]outgoing
		cur  []outgoing
		size int
	)
	for _, o := range events {
		n := utf8.RuneCountInString(o.event.Message)
		if n > budget {
			o.event.Message = string([]rune(o.event.Message)[:budget-1]) + "…"
			n = budget
		}
		if len(cur) > 0 && size+2+n > budget {
			out = append(out, cur)
			cur, size = nil, 0
		}
		if len(cur) > 0 {
			size += 2 // the blank line between messages
		}
		cur = append(cur, o)
		size += n
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler processes decoded messages. Returning nil means they are durably
// stored and their offsets may be committed.
type Handler[T any] func(ctx context.Context, items []T) error

// ConsumerConfig is generic over the message type: scrape batches and log
// batches go through the same at-least-once machinery, only the decoding
// and the handler differ.
type ConsumerConfig[T any] struct {
	Brokers []string
	Topic   string
	Group   string
	Decode  func([]byte) (T, error)
	Handle  Handler[T]
	// Permanent reports errors that no retry can fix, such as a value the
	// database rejects. Without it one bad batch would be retried forever
	// and block its partition. Nil means every error is retried.
	Permanent func(error) bool
}

const (
	// maxPollRecords bounds one handler call: one record is one scrape of
	// one target, a few hundred samples, so this caps a write at roughly
	// a hundred thousand rows.
	maxPollRecords    = 200
	defaultMinBackoff = 500 * time.Millisecond
	defaultMaxBackoff = 10 * time.Second
)

type Consumer[T any] struct {
	client     *kgo.Client
	topic      string
	decode     func([]byte) (T, error)
	handle     Handler[T]
	permanent  func(error) bool
	log        *slog.Logger
	minBackoff time.Duration
	maxBackoff time.Duration
}

func NewConsumer[T any](cfg ConsumerConfig[T], log *slog.Logger) (*Consumer[T], error) {
	if cfg.Handle == nil || cfg.Decode == nil {
		return nil, fmt.Errorf("consumer for %s: nil handler or decoder", cfg.Topic)
	}
	permanent := cfg.Permanent
	if permanent == nil {
		permanent = func(error) bool { return false }
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		// Offsets are committed by hand, only after the handler succeeded.
		// Autocommit would commit on a timer, possibly before the write,
		// and a crash in between would lose those records.
		kgo.DisableAutoCommit(),
		// A brand-new group starts from the oldest retained record, so data
		// produced before the first processor ever ran is not skipped.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer[T]{
		client:     client,
		topic:      cfg.Topic,
		decode:     cfg.Decode,
		handle:     cfg.Handle,
		permanent:  permanent,
		log:        log,
		minBackoff: defaultMinBackoff,
		maxBackoff: defaultMaxBackoff,
	}, nil
}

// Run consumes until pollCtx is cancelled.
//
// The two contexts split graceful shutdown in two: cancelling pollCtx stops
// fetching new records, while the batch already in hand keeps being written
// and committed under workCtx. workCtx should be cancelled only when the
// shutdown deadline expires; whatever is uncommitted at that point is simply
// redelivered on the next start.
func (c *Consumer[T]) Run(pollCtx, workCtx context.Context) {
	for {
		fetches := c.client.PollRecords(pollCtx, maxPollRecords)
		if pollCtx.Err() != nil || fetches.IsClientClosed() {
			// Records returned together with the cancellation are not
			// committed, so dropping them here loses nothing.
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn("fetch error", "topic", topic, "partition", partition, "err", err)
		})

		if records := fetches.Records(); len(records) > 0 {
			consumed.WithLabelValues(c.topic).Add(float64(len(records)))
			start := time.Now()
			c.process(workCtx, records)
			handleDuration.WithLabelValues(c.topic).Observe(time.Since(start).Seconds())
		}
	}
}

// process gives at-least-once delivery: offsets are committed strictly
// after the handler succeeded. A crash between the two redelivers the
// records, which is why the handler must be idempotent.
func (c *Consumer[T]) process(ctx context.Context, records []*kgo.Record) {
	batches := make([]T, 0, len(records))
	for _, r := range records {
		b, err := c.decode(r.Value)
		if err != nil {
			// Redelivery cannot fix a malformed record, so it is skipped
			// and its offset committed along with the rest.
			c.log.Error("skip undecodable record", "partition", r.Partition, "offset", r.Offset, "err", err)
			dropped.WithLabelValues(c.topic, "undecodable").Inc()
			continue
		}
		batches = append(batches, b)
	}

	if len(batches) > 0 {
		if err := c.handleWithRetry(ctx, batches); err != nil {
			c.log.Warn("records left uncommitted, they will be redelivered", "records", len(records), "err", err)
			return
		}
	}

	if err := c.client.CommitRecords(ctx, records...); err != nil {
		// The data is already stored; a failed commit only means these
		// records come again and the idempotent write absorbs them.
		c.log.Warn("commit offsets", "records", len(records), "err", err)
		commitFailures.WithLabelValues(c.topic).Inc()
		return
	}
	c.log.Debug("committed", "records", len(records))
}

// handleWithRetry retries transient errors with exponential backoff until
// ctx expires. On a permanent error it narrows down to single batches so
// that only the offending one is dropped, not every batch polled with it.
func (c *Consumer[T]) handleWithRetry(ctx context.Context, batches []T) error {
	backoff := c.minBackoff
	for attempt := 1; ; attempt++ {
		err := c.handle(ctx, batches)
		if err == nil {
			return nil
		}

		if c.permanent(err) {
			if len(batches) == 1 {
				c.log.Error("drop message rejected permanently", "err", err)
				dropped.WithLabelValues(c.topic, "permanent").Inc()
				return nil
			}
			for _, b := range batches {
				if err := c.handleWithRetry(ctx, []T{b}); err != nil {
					return err
				}
			}
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		handleRetries.WithLabelValues(c.topic).Inc()
		c.log.Warn("handle batches, retrying", "attempt", attempt, "batches", len(batches),
			"backoff", backoff.String(), "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(backoff*2, c.maxBackoff)
	}
}

// Close leaves the consumer group, so its partitions are reassigned right
// away instead of after the session timeout. Leaving is bounded by ctx:
// with the broker unreachable a plain Close waited for the leave forever,
// past any shutdown deadline (seen when the container VM hung under load).
// If the leave times out, the broker drops the member after its session
// timeout instead.
func (c *Consumer[T]) Close(ctx context.Context) {
	if err := c.client.LeaveGroupContext(ctx); err != nil {
		c.log.Warn("leave consumer group", "topic", c.topic, "err", err)
	}
	c.client.Close()
}

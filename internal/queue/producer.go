package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/te1pe/foresight/internal/logs"
	"github.com/te1pe/foresight/internal/scrape"
)

// maxBufferedBytes bounds memory while the broker is unreachable. Once it
// is full, Produce blocks, the scrape channel fills up and scrapers stall:
// backpressure instead of unbounded growth or silent drops.
const maxBufferedBytes = 64 << 20

type Producer struct {
	client       *kgo.Client
	samplesTopic string
	logsTopic    string
	log          *slog.Logger
}

type ProducerConfig struct {
	Brokers      []string
	SamplesTopic string
	LogsTopic    string
}

// NewProducer relies on franz-go defaults that matter for delivery:
// idempotent production (broker de-duplicates our own retries), acks from
// all in-sync replicas, unlimited retries and no delivery timeout. A record
// is therefore only lost if the process stops before the broker acks it.
func NewProducer(cfg ProducerConfig, log *slog.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.SamplesTopic),
		kgo.MaxBufferedBytes(maxBufferedBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client, samplesTopic: cfg.SamplesTopic, logsTopic: cfg.LogsTopic, log: log}, nil
}

// Forward publishes batches until in is closed. Produce is asynchronous, so
// Forward returning means "handed to the client", not "acknowledged";
// call Flush for that.
//
// ctx is attached to every record and cancelling it fails records that are
// still buffered, so it must outlive the scrapers and be cancelled only
// when the shutdown deadline runs out.
func (p *Producer) Forward(ctx context.Context, in <-chan scrape.Batch) {
	for b := range in {
		value, err := Encode(b)
		if err != nil {
			p.log.Error("drop batch", "target", b.Target, "err", err)
			continue
		}
		// Keying by target keeps each target on one partition, so its batches
		// stay ordered and a consumer sees one target's series contiguously.
		rec := &kgo.Record{Key: []byte(b.Target), Value: value}
		samples := len(b.Samples)
		p.client.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				published.WithLabelValues(p.samplesTopic, "error").Inc()
				p.log.Error("publish batch", "target", b.Target, "samples", samples, "err", err)
				return
			}
			published.WithLabelValues(p.samplesTopic, "ok").Inc()
			p.log.Debug("published batch", "target", b.Target, "samples", samples,
				"partition", r.Partition, "offset", r.Offset)
		})
	}
}

// PublishLogs is synchronous, unlike Forward: the HTTP client that sent the
// logs is answered only after the broker acknowledged them, so a success
// response means the logs are durable and an error means "retry".
func (p *Producer) PublishLogs(ctx context.Context, b logs.Batch) error {
	value, err := EncodeLogs(b)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: p.logsTopic, Key: []byte(b.Service), Value: value}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		published.WithLabelValues(p.logsTopic, "error").Inc()
		return fmt.Errorf("publish logs of %s: %w", b.Service, err)
	}
	published.WithLabelValues(p.logsTopic, "ok").Inc()
	return nil
}

// Flush waits until every produced record is acknowledged or failed.
func (p *Producer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("flush kafka producer: %w", err)
	}
	return nil
}

func (p *Producer) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return nil
}

func (p *Producer) Close() { p.client.Close() }

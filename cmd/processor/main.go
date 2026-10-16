// Command processor consumes scrape batches and pushed logs from Redpanda,
// stores them in ClickHouse with at-least-once delivery and idempotent
// writes, and once a minute evaluates detection rules over the stored
// windows.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/te1pe/foresight/internal/admin"
	"github.com/te1pe/foresight/internal/config"
	"github.com/te1pe/foresight/internal/logs"
	"github.com/te1pe/foresight/internal/queue"
	"github.com/te1pe/foresight/internal/rules"
	"github.com/te1pe/foresight/internal/scrape"
	"github.com/te1pe/foresight/internal/storage/clickhouse"
)

const (
	// adminStopTimeout bounds the wait for a pprof download in progress.
	adminStopTimeout = 2 * time.Second
	shutdownTimeout  = 10 * time.Second
	startupPing      = 5 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("processor stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("processor stopped")
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Diagnostics come up first and go down last, so metrics and pprof stay
	// available through startup and graceful shutdown.
	adminAddr := cfg.AdminAddr
	if adminAddr == "" {
		adminAddr = "127.0.0.1:6061"
	}
	stopAdmin := admin.Start(adminAddr, log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), adminStopTimeout)
		defer cancel()
		if err := stopAdmin(ctx); err != nil {
			log.Warn("stop admin server", "err", err)
		}
	}()

	store, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		return err
	}
	// Not fatal: until the database answers, the consumer retries the
	// first batch without committing, so nothing is skipped.
	pingCtx, cancelPing := context.WithTimeout(context.Background(), startupPing)
	if err := store.Ping(pingCtx); err != nil {
		log.Warn("clickhouse is not reachable yet", "err", err)
	}
	cancelPing()

	consumer, err := queue.NewConsumer(queue.ConsumerConfig[scrape.Batch]{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		Group:   cfg.KafkaGroup,
		Decode:  queue.Decode,
		Handle: func(ctx context.Context, batches []scrape.Batch) error {
			rows, err := store.InsertBatches(ctx, batches)
			if err != nil {
				return err
			}
			// ClickHouse drops redelivered scrapes silently, so rows is what
			// was sent, not what was new.
			log.Info("stored samples", "batches", len(batches), "rows", rows)
			return nil
		},
		// No Permanent classifier: the native protocol sends typed columns
		// built from already-validated Go values, so there is no class of
		// "this data is unacceptable" errors here. Every failure — server
		// down, too many parts, missing table — is worth retrying.
	}, log)
	if err != nil {
		store.Close()
		return err
	}
	// Logs get their own consumer group: a slow log write must not hold back
	// the metrics that alerts depend on, and the two lags are visible apart.
	logConsumer, err := queue.NewConsumer(queue.ConsumerConfig[logs.Batch]{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaLogsTopic,
		Group:   cfg.KafkaGroup + "-logs",
		Decode:  queue.DecodeLogs,
		Handle: func(ctx context.Context, batches []logs.Batch) error {
			rows, err := store.InsertLogs(ctx, batches)
			if err != nil {
				return err
			}
			log.Info("stored logs", "batches", len(batches), "rows", rows)
			return nil
		},
	}, log)
	if err != nil {
		consumer.Close(context.Background())
		store.Close()
		return err
	}

	// Same split as in ingest: pollCtx stops taking new records on a
	// signal, workCtx lets the batch in hand finish and is cancelled only
	// when the shutdown deadline expires.
	pollCtx, stopPolling := context.WithCancel(context.Background())
	defer stopPolling()
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		consumer.Run(pollCtx, workCtx)
	}()
	logConsumerDone := make(chan struct{})
	go func() {
		defer close(logConsumerDone)
		logConsumer.Run(pollCtx, workCtx)
	}()

	// Rules read from ClickHouse rather than from the consumed stream: the
	// stored windows are already deduplicated and survive restarts, so the
	// evaluator keeps no state of its own. It stops on the same signal as
	// polling; an interrupted round is simply evaluated again next time.
	evaluator := rules.NewEvaluator(store, rules.Default, log)
	evaluatorDone := make(chan struct{})
	go func() {
		defer close(evaluatorDone)
		evaluator.Run(pollCtx)
	}()

	log.Info("processor started", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "logs_topic", cfg.KafkaLogsTopic, "group", cfg.KafkaGroup)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	<-sigCtx.Done()
	log.Info("shutdown: signal received")
	stopSignals()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	stopAbort := context.AfterFunc(ctx, cancelWork)
	defer stopAbort()

	var errs []error
	log.Info("shutdown: stopping consumer and rule evaluation, finishing the batch in progress")
	stopPolling()
	if err := waitFor(ctx, consumerDone); err != nil {
		errs = append(errs, fmt.Errorf("stop consumer: %w", err))
	} else {
		log.Info("shutdown: consumer stopped")
	}
	if err := waitFor(ctx, logConsumerDone); err != nil {
		errs = append(errs, fmt.Errorf("stop log consumer: %w", err))
	} else {
		log.Info("shutdown: log consumer stopped")
	}
	if err := waitFor(ctx, evaluatorDone); err != nil {
		errs = append(errs, fmt.Errorf("stop rule evaluation: %w", err))
	} else {
		log.Info("shutdown: rule evaluation stopped")
	}

	// Leaving the group explicitly lets other processors take over the
	// partitions right away instead of after the session timeout. The
	// ClickHouse connection goes last because the consumer's writes use it.
	log.Info("shutdown: leaving consumer groups")
	consumer.Close(ctx)
	logConsumer.Close(ctx)

	log.Info("shutdown: closing clickhouse connection")
	if err := store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close clickhouse: %w", err))
	}

	return errors.Join(errs...)
}

func waitFor(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

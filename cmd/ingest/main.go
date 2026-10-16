// Command ingest scrapes the targets listed in PostgreSQL, accepts pushed
// logs over HTTP, and publishes both to Redpanda.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/te1pe/foresight/internal/admin"
	"github.com/te1pe/foresight/internal/config"
	"github.com/te1pe/foresight/internal/httpapi"
	"github.com/te1pe/foresight/internal/queue"
	"github.com/te1pe/foresight/internal/scrape"
	"github.com/te1pe/foresight/internal/storage/postgres"
)

const (
	// adminStopTimeout bounds the wait for a pprof download in progress.
	adminStopTimeout  = 2 * time.Second
	scrapeTimeout     = 10 * time.Second
	shutdownTimeout   = 10 * time.Second
	startupPing       = 5 * time.Second
	readHeaderTimeout = 5 * time.Second
	// Room for several scrapes of every target while the producer is busy;
	// when it fills up, scrapers block, which is the backpressure we want.
	batchQueueSize = 64
	// targetsReload is how quickly an INSERT into the targets table takes
	// effect.
	targetsReload = 30 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("ingest stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("ingest stopped")
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
		adminAddr = "127.0.0.1:6060"
	}
	stopAdmin := admin.Start(adminAddr, log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), adminStopTimeout)
		defer cancel()
		if err := stopAdmin(ctx); err != nil {
			log.Warn("stop admin server", "err", err)
		}
	}()

	producer, err := queue.NewProducer(queue.ProducerConfig{
		Brokers: cfg.KafkaBrokers, SamplesTopic: cfg.KafkaTopic, LogsTopic: cfg.KafkaLogsTopic,
	}, log)
	if err != nil {
		return err
	}
	defer producer.Close()
	// Only the target list is read from PostgreSQL. If it is down at start,
	// nothing is scraped until it answers; if it goes down later, the last
	// list keeps being scraped.
	db, err := postgres.New(cfg.DBDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	// An unreachable broker is not fatal: the producer keeps retrying in the
	// background, scraping and /v1/targets still work, /readyz reports it.
	pingCtx, cancelPing := context.WithTimeout(context.Background(), startupPing)
	if err := producer.Ping(pingCtx); err != nil {
		log.Warn("kafka is not reachable yet", "err", err)
	}
	cancelPing()

	batches := make(chan scrape.Batch, batchQueueSize)
	scraper, err := scrape.New(scrape.Config{
		Interval: cfg.ScrapeInterval,
		Timeout:  scrapeTimeout,
	}, batches, log)
	if err != nil {
		return fmt.Errorf("create scraper: %w", err)
	}

	// Publishing has its own context, not the scrapers' one: on shutdown it
	// must keep going after scrapers are cancelled in order to deliver what
	// they produced. It is cancelled only if the shutdown deadline hits, and
	// that also fails records still buffered in the producer.
	publishCtx, cancelPublish := context.WithCancel(context.Background())
	defer cancelPublish()
	forwarderDone := make(chan struct{})
	go func() {
		defer close(forwarderDone)
		producer.Forward(publishCtx, batches)
	}()

	// The watcher and the scrapers share one context: on shutdown both stop
	// together, and the scraper stops all its target goroutines.
	scrapeCtx, stopScrapers := context.WithCancel(context.Background())
	defer stopScrapers()
	targetUpdates := make(chan []string)
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		scrape.WatchTargets(scrapeCtx, db.EnabledTargets, targetsReload, targetUpdates, log)
	}()
	scrapersDone := make(chan struct{})
	go func() {
		defer close(scrapersDone)
		scraper.Run(scrapeCtx, targetUpdates)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewIngestHandler(producer, scraper, producer, log),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()

	log.Info("ingest started", "http_addr", cfg.HTTPAddr, "scrape_interval", cfg.ScrapeInterval.String())

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	var errs []error
	select {
	case <-sigCtx.Done():
		log.Info("shutdown: signal received")
	case err := <-serverErr:
		errs = append(errs, fmt.Errorf("http server: %w", err))
	}
	// Restore default signal handling: a second Ctrl+C kills the process
	// immediately if graceful shutdown gets stuck.
	stopSignals()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	stopAbort := context.AfterFunc(ctx, cancelPublish)
	defer stopAbort()

	// Order matters: scrapers and the target watcher first, then the
	// forwarder they feed, then the producer's own buffer, then the HTTP
	// server (so /v1/targets, /readyz and log pushes keep working to the
	// very end), and the Kafka and PostgreSQL clients last, in the deferred
	// calls, because the forwarder and the handlers use them.
	log.Info("shutdown: stopping scrapers")
	stopScrapers()
	if err := waitFor(ctx, watcherDone); err != nil {
		errs = append(errs, fmt.Errorf("stop target watcher: %w", err))
	}
	if err := waitFor(ctx, scrapersDone); err != nil {
		errs = append(errs, fmt.Errorf("stop scrapers: %w", err))
	} else {
		// Closing is safe only now: no scraper can send anymore.
		close(batches)
		log.Info("shutdown: scrapers stopped, publishing queued batches", "queued", len(batches))
		if err := waitFor(ctx, forwarderDone); err != nil {
			errs = append(errs, fmt.Errorf("forward batches: %w", err))
		} else if err := producer.Flush(ctx); err != nil {
			errs = append(errs, err)
		} else {
			log.Info("shutdown: all batches acknowledged by kafka")
		}
	}

	log.Info("shutdown: stopping http server")
	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
		srv.Close()
	}

	log.Info("shutdown: closing kafka and postgres clients")
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

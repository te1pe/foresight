// Command query serves read access to stored series over HTTP.
//
// It is separate from ingest and processor so that the read path scales and
// fails on its own: a heavy query can slow this service down, but it cannot
// stall scraping or consumption.
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
	"github.com/te1pe/foresight/internal/storage/clickhouse"
)

const (
	// adminStopTimeout bounds the wait for a pprof download in progress.
	adminStopTimeout  = 2 * time.Second
	shutdownTimeout   = 10 * time.Second
	startupPing       = 5 * time.Second
	readHeaderTimeout = 5 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("query stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("query stopped")
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
		adminAddr = "127.0.0.1:6062"
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
	pingCtx, cancelPing := context.WithTimeout(context.Background(), startupPing)
	if err := store.Ping(pingCtx); err != nil {
		log.Warn("clickhouse is not reachable yet", "err", err)
	}
	cancelPing()

	srv := &http.Server{
		Addr:              cfg.QueryHTTPAddr,
		Handler:           httpapi.NewQueryHandler(store, store, log),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()
	log.Info("query started", "http_addr", cfg.QueryHTTPAddr)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	var errs []error
	select {
	case <-sigCtx.Done():
		log.Info("shutdown: signal received")
	case err := <-serverErr:
		errs = append(errs, fmt.Errorf("http server: %w", err))
	}
	stopSignals()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// In-flight queries finish (each is bounded by its own timeout) before
	// the connection they read through is closed.
	log.Info("shutdown: stopping http server")
	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
		srv.Close()
	}

	log.Info("shutdown: closing clickhouse connection")
	if err := store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close clickhouse: %w", err))
	}
	return errors.Join(errs...)
}

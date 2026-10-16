// Command alerter turns detections into incidents (PostgreSQL) and delivers
// notifications about them (Telegram), deduplicated through Redis.
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
	"github.com/te1pe/foresight/internal/alert"
	"github.com/te1pe/foresight/internal/config"
	"github.com/te1pe/foresight/internal/notify"
	"github.com/te1pe/foresight/internal/storage/clickhouse"
	"github.com/te1pe/foresight/internal/storage/postgres"
	"github.com/te1pe/foresight/internal/storage/redis"
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
		log.Error("alerter stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("alerter stopped")
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
		adminAddr = "127.0.0.1:6063"
	}
	stopAdmin := admin.Start(adminAddr, log)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), adminStopTimeout)
		defer cancel()
		if err := stopAdmin(ctx); err != nil {
			log.Warn("stop admin server", "err", err)
		}
	}()

	detections, err := clickhouse.New(cfg.ClickHouseDSN)
	if err != nil {
		return err
	}
	defer detections.Close()
	incidents, err := postgres.New(cfg.DBDSN)
	if err != nil {
		return err
	}
	defer incidents.Close()
	claims := redis.New(cfg.RedisAddr)
	defer claims.Close()

	// None of the dependencies is required at startup: each round reads
	// durable state and simply fails until everything answers.
	pingCtx, cancelPing := context.WithTimeout(context.Background(), startupPing)
	for name, ping := range map[string]func(context.Context) error{
		"clickhouse": detections.Ping, "postgres": incidents.Ping, "redis": claims.Ping,
	} {
		if err := ping(pingCtx); err != nil {
			log.Warn("dependency is not reachable yet", "dependency", name, "err", err)
		}
	}
	cancelPing()

	var notifier alert.Notifier
	if cfg.TelegramToken != "" {
		notifier = notify.NewTelegram(cfg.TelegramAPIURL, cfg.TelegramToken, cfg.TelegramChatID)
		// The chat id is not a secret; the token is never logged.
		log.Info("notifications go to telegram", "chat_id", cfg.TelegramChatID)
	} else {
		notifier = notify.NewLog(log)
		log.Warn("FORESIGHT_TELEGRAM_TOKEN is not set; notifications go to the log")
	}

	a := alert.New(alert.DefaultConfig, detections, incidents, claims, notifier, log)

	// Same split as in the other services: a signal stops new rounds, the
	// round in progress finishes unless the shutdown deadline cuts it off.
	stopCtx, stop := context.WithCancel(context.Background())
	defer stop()
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Run(stopCtx, workCtx)
	}()
	log.Info("alerter started", "poll_interval", alert.DefaultConfig.PollInterval.String())

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	<-sigCtx.Done()
	log.Info("shutdown: signal received")
	stopSignals()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	stopAbort := context.AfterFunc(ctx, cancelWork)
	defer stopAbort()

	log.Info("shutdown: finishing the current round")
	stop()
	var errs []error
	select {
	case <-done:
		log.Info("shutdown: alerter stopped")
	case <-ctx.Done():
		errs = append(errs, fmt.Errorf("stop alerter: %w", ctx.Err()))
	}
	// Connections are closed by the deferred calls, after the round that
	// uses them is over.
	log.Info("shutdown: closing connections")
	return errors.Join(errs...)
}

// Command loadgen puts synthetic load on a running foresight stack and
// prints what foresight's own metrics say about it. See README, "Нагрузка".
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/te1pe/foresight/internal/config"
	"github.com/te1pe/foresight/internal/loadgen"
	"github.com/te1pe/foresight/internal/storage/postgres"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("loadgen failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadgen.LoadConfig(os.Getenv)
	if err != nil {
		return err
	}
	svc, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	db, err := postgres.New(svc.DBDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	// Ctrl+C ends the run early; the synthetic targets are still removed.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	report, err := loadgen.Run(ctx, cfg, db, log)
	if err != nil {
		return err
	}
	fmt.Print(report)
	return nil
}

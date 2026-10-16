// Package admin serves a service's own diagnostics: Prometheus metrics and
// pprof profiles.
package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewServer returns the diagnostics server for addr.
//
// It is a separate listener rather than extra routes on the public API:
// pprof can dump goroutine stacks and the heap, so it must never be
// reachable from outside, and the default addresses bind to 127.0.0.1.
// Note that importing net/http/pprof registers its handlers on
// http.DefaultServeMux in init(), whatever we do here; what keeps them off
// the public API is that the public servers use their own muxes (checked
// by the httpapi tests).
func NewServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// No WriteTimeout: a CPU profile streams for as long as asked
		// (?seconds=30), and a timeout would cut it off.
	}
}

// Start serves diagnostics on addr in the background and returns the
// function that stops it and waits for the serving goroutine to exit.
//
// A failure to listen (port taken) is logged, not fatal: diagnostics are
// worth less than the data path they observe.
func Start(addr string, log *slog.Logger) (stop func(context.Context) error) {
	srv := NewServer(addr)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server", "addr", addr, "err", err)
		}
	}()
	log.Info("admin server started", "addr", addr)
	return func(ctx context.Context) error {
		err := srv.Shutdown(ctx)
		<-done
		return err
	}
}

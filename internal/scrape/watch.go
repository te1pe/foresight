package scrape

import (
	"context"
	"log/slog"
	"slices"
	"time"
)

// WatchTargets loads the target list now and then every interval, and sends
// it to out whenever it differs from the last list sent. It returns when ctx
// is cancelled.
//
// A failed load keeps the previous list: the targets table lives in
// PostgreSQL, and a database outage must not stop scraping the hosts we
// already know about. Until the first successful load nothing is scraped.
func WatchTargets(ctx context.Context, load func(context.Context) ([]string, error), interval time.Duration, out chan<- []string, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var last []string
	sent := false
	for {
		names, err := load(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Warn("load targets; keeping the previous list", "err", err)
		case err == nil:
			names = slices.Clone(names)
			slices.Sort(names)
			names = slices.Compact(names)
			if !sent || !slices.Equal(names, last) {
				log.Info("targets loaded", "targets", names)
				select {
				case out <- names:
					last, sent = names, true
				case <-ctx.Done():
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

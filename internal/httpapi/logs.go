package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/te1pe/foresight/internal/logs"
)

type LogPublisher interface {
	PublishLogs(ctx context.Context, b logs.Batch) error
}

const (
	// maxLogsBody keeps one request, re-encoded with its parsed fields,
	// well under the broker's 1 MB message limit.
	maxLogsBody    = 256 << 10
	publishTimeout = 10 * time.Second
)

// serviceName is restrictive on purpose: it becomes a Kafka key and a
// ClickHouse LowCardinality value, and a typo'd free-form name would
// silently start a new log stream.
var serviceName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,64}$`)

type logsResponse struct {
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected"`
	Error    string `json:"error,omitempty"`
}

// postLogs serves
//
//	POST /v1/logs?service=NAME   (body: newline-delimited JSON, e.g. slog output)
//
// 202 means the accepted lines are durable in the queue. 503 means nothing
// was stored and the client should retry; repeating lines is safe because
// storage deduplicates them by content.
func postLogs(pub LogPublisher, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		service := r.URL.Query().Get("service")
		if !serviceName.MatchString(service) {
			writeError(w, http.StatusBadRequest, "service must match "+serviceName.String(), log)
			return
		}

		body := http.MaxBytesReader(w, r.Body, maxLogsBody)
		b, rejected, err := logs.Parse(body, service, time.Now().UTC())
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				writeError(w, http.StatusRequestEntityTooLarge, "body over 256 KiB: split it into several requests", log)
				return
			}
			writeError(w, http.StatusBadRequest, err.Error(), log)
			return
		}
		if len(b.Entries) == 0 {
			logLines.WithLabelValues("rejected").Add(float64(rejected))
			writeJSON(w, http.StatusBadRequest, logsResponse{Rejected: rejected, Error: "no valid log lines"}, log)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), publishTimeout)
		defer cancel()
		if err := pub.PublishLogs(ctx, b); err != nil {
			log.Error("publish logs", "service", service, "entries", len(b.Entries), "err", err)
			writeJSON(w, http.StatusServiceUnavailable, logsResponse{Error: "logs not stored, retry later"}, log)
			return
		}
		logLines.WithLabelValues("accepted").Add(float64(len(b.Entries)))
		logLines.WithLabelValues("rejected").Add(float64(rejected))
		writeJSON(w, http.StatusAccepted, logsResponse{Accepted: len(b.Entries), Rejected: rejected}, log)
	}
}

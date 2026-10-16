package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	chstore "github.com/te1pe/foresight/internal/storage/clickhouse"
)

// Reader is the read side of storage used by the query service.
type Reader interface {
	QuerySeries(ctx context.Context, q chstore.SeriesQuery) ([]chstore.Series, error)
	LatestDetections(ctx context.Context, target string, since time.Time) ([]chstore.Detection, error)
}

const (
	// maxPoints bounds the memory one request can make the service hold.
	// A larger answer is refused rather than truncated: a silently cut
	// series looks like real data and would mislead a chart or a detector.
	maxPoints    = 50_000
	defaultRange = time.Hour
	queryTimeout = 15 * time.Second
	// detectionsFreshness hides series that stopped being evaluated (a
	// target removed, a disk unmounted) instead of showing their last,
	// stale verdict forever.
	detectionsFreshness = 15 * time.Minute
)

func NewQueryHandler(store Reader, dep Pinger, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz(log))
	mux.HandleFunc("GET /readyz", readyz(dep, log))
	mux.HandleFunc("GET /v1/series", querySeries(store, log))
	mux.HandleFunc("GET /v1/detections", listDetections(store, log))
	return instrument(mux)
}

// Values travel as strings, as in the queue: JSON numbers cannot hold the
// NaN and ±Inf that metrics legitimately contain. The series id is a string
// because a uint64 does not fit in a JavaScript number.
type seriesJSON struct {
	ID      string            `json:"id"`
	Labels  map[string]string `json:"labels"`
	Type    string            `json:"type"`
	Points  []pointJSON       `json:"points,omitempty"`
	Windows []windowJSON      `json:"windows,omitempty"`
}

type pointJSON struct {
	T time.Time `json:"t"`
	V string    `json:"v"`
}

type windowJSON struct {
	T     time.Time `json:"t"`
	Min   string    `json:"min"`
	Max   string    `json:"max"`
	Avg   string    `json:"avg"`
	Last  string    `json:"last"`
	Count uint64    `json:"count"`
}

type seriesResponse struct {
	Target string       `json:"target"`
	Name   string       `json:"name"`
	Step   string       `json:"step"`
	From   time.Time    `json:"from"`
	To     time.Time    `json:"to"`
	Series []seriesJSON `json:"series"`
}

// querySeries serves
//
//	GET /v1/series?target=T&name=N[&step=raw|1m|5m][&from=RFC3339][&to=RFC3339][&label=k=v ...]
//
// step defaults to raw, to to now and from to an hour before to.
func querySeries(store Reader, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q, err := parseSeriesQuery(r.URL.Query(), time.Now())
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), log)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
		defer cancel()
		series, err := store.QuerySeries(ctx, q)
		switch {
		case errors.Is(err, chstore.ErrTooManyPoints):
			writeError(w, http.StatusUnprocessableEntity,
				fmt.Sprintf("more than %d points: narrow the range, add label filters or use a coarser step", maxPoints), log)
			return
		case err != nil:
			log.Error("query series", "target", q.Target, "name", q.Name, "step", q.Step, "err", err)
			writeError(w, http.StatusInternalServerError, "query failed", log)
			return
		}

		resp := seriesResponse{
			Target: q.Target, Name: q.Name, Step: string(q.Step), From: q.From, To: q.To,
			Series: make([]seriesJSON, 0, len(series)),
		}
		for _, s := range series {
			resp.Series = append(resp.Series, toSeriesJSON(s))
		}
		writeJSON(w, http.StatusOK, resp, log)
	}
}

func parseSeriesQuery(v url.Values, now time.Time) (chstore.SeriesQuery, error) {
	q := chstore.SeriesQuery{
		Target:    v.Get("target"),
		Name:      v.Get("name"),
		Step:      chstore.Step(v.Get("step")),
		MaxPoints: maxPoints,
	}
	if q.Target == "" || q.Name == "" {
		return q, errors.New("target and name are required")
	}
	if q.Step == "" {
		q.Step = chstore.StepRaw
	}
	if !q.Step.Valid() {
		return q, fmt.Errorf("step must be raw, 1m or 5m, got %q", q.Step)
	}

	var err error
	q.To = now.UTC()
	if raw := v.Get("to"); raw != "" {
		if q.To, err = time.Parse(time.RFC3339, raw); err != nil {
			return q, fmt.Errorf("to: %w", err)
		}
	}
	q.From = q.To.Add(-defaultRange)
	if raw := v.Get("from"); raw != "" {
		if q.From, err = time.Parse(time.RFC3339, raw); err != nil {
			return q, fmt.Errorf("from: %w", err)
		}
	}
	if !q.From.Before(q.To) {
		return q, errors.New("from must be before to")
	}

	for _, l := range v["label"] {
		k, val, ok := strings.Cut(l, "=")
		if !ok || k == "" {
			return q, fmt.Errorf("label must be name=value, got %q", l)
		}
		if q.Labels == nil {
			q.Labels = make(map[string]string)
		}
		q.Labels[k] = val
	}
	return q, nil
}

func toSeriesJSON(s chstore.Series) seriesJSON {
	out := seriesJSON{ID: strconv.FormatUint(s.ID, 10), Labels: s.Labels, Type: s.Type}
	for _, p := range s.Points {
		out.Points = append(out.Points, pointJSON{T: p.Time, V: formatFloat(p.Value)})
	}
	for _, w := range s.Windows {
		out.Windows = append(out.Windows, windowJSON{
			T: w.Start, Min: formatFloat(w.Min), Max: formatFloat(w.Max),
			Avg: formatFloat(w.Avg), Last: formatFloat(w.Last), Count: w.Count,
		})
	}
	return out
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

type detectionJSON struct {
	EvalTime time.Time         `json:"eval_time"`
	Rule     string            `json:"rule"`
	Kind     string            `json:"kind"`
	Status   string            `json:"status"`
	Firing   bool              `json:"firing"`
	Target   string            `json:"target"`
	Name     string            `json:"name"`
	SeriesID string            `json:"series_id"`
	Labels   map[string]string `json:"labels"`
	Observed string            `json:"observed"`
	Expected string            `json:"expected"`
	Score    string            `json:"score"`
	Slope    string            `json:"slope_per_second,omitempty"`
	// ETA is both machine- and human-readable: seconds for clients,
	// "3h20m" for whoever reads the JSON.
	ETASeconds *float64 `json:"eta_seconds,omitempty"`
	ETA        string   `json:"eta,omitempty"`
	Points     int      `json:"points"`
}

// listDetections serves
//
//	GET /v1/detections[?target=T][&firing=true]
//
// with the latest evaluation of every rule and series from the last
// detectionsFreshness.
func listDetections(store Reader, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		target := r.URL.Query().Get("target")
		onlyFiring := r.URL.Query().Get("firing") == "true"

		ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
		defer cancel()
		ds, err := store.LatestDetections(ctx, target, time.Now().Add(-detectionsFreshness))
		if err != nil {
			log.Error("list detections", "target", target, "err", err)
			writeError(w, http.StatusInternalServerError, "query failed", log)
			return
		}

		out := make([]detectionJSON, 0, len(ds))
		for _, d := range ds {
			if onlyFiring && !d.Firing {
				continue
			}
			dj := detectionJSON{
				EvalTime: d.EvalTime, Rule: d.Rule, Kind: d.Kind, Status: d.Status, Firing: d.Firing,
				Target: d.Target, Name: d.Name, SeriesID: strconv.FormatUint(d.SeriesID, 10), Labels: d.Labels,
				Observed: formatFloat(d.Observed), Expected: formatFloat(d.Expected), Score: formatFloat(d.Score),
				Points: d.Points,
			}
			if d.Slope != 0 {
				dj.Slope = formatFloat(d.Slope)
			}
			if d.ETA != nil {
				sec := d.ETA.Seconds()
				dj.ETASeconds, dj.ETA = &sec, formatETA(*d.ETA)
			}
			out = append(out, dj)
		}
		writeJSON(w, http.StatusOK, map[string]any{"detections": out}, log)
	}
}

// formatETA rounds to minutes and spells out days, which Duration.String
// does not: "2d3h20m" rather than "51h20m0s".
func formatETA(d time.Duration) string {
	d = d.Round(time.Minute)
	if d < time.Minute {
		return "now"
	}
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	hours := d / time.Hour
	minutes := (d - hours*time.Hour) / time.Minute

	var sb strings.Builder
	if days > 0 {
		fmt.Fprintf(&sb, "%dd", days)
	}
	if hours > 0 {
		fmt.Fprintf(&sb, "%dh", hours)
	}
	if minutes > 0 {
		fmt.Fprintf(&sb, "%dm", minutes)
	}
	return sb.String()
}

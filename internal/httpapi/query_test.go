package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	chstore "github.com/te1pe/foresight/internal/storage/clickhouse"
)

type fakeQuerier struct {
	got        chstore.SeriesQuery
	series     []chstore.Series
	err        error
	detections []chstore.Detection
	gotTarget  string
	gotSince   time.Time
}

func (f *fakeQuerier) LatestDetections(_ context.Context, target string, since time.Time) ([]chstore.Detection, error) {
	f.gotTarget, f.gotSince = target, since
	return f.detections, f.err
}

func (f *fakeQuerier) QuerySeries(_ context.Context, q chstore.SeriesQuery) ([]chstore.Series, error) {
	f.got = q
	return f.series, f.err
}

func newQueryTestHandler(f *fakeQuerier) http.Handler {
	return NewQueryHandler(f, fakePinger{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestParseSeriesQuery(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	from := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		query   string
		want    chstore.SeriesQuery
		wantErr string
	}{
		{
			name:  "defaults",
			query: "target=t&name=m",
			want:  chstore.SeriesQuery{Target: "t", Name: "m", Step: chstore.StepRaw, From: now.Add(-time.Hour), To: now, MaxPoints: maxPoints},
		},
		{
			name:  "everything set",
			query: "target=t&name=m&step=5m&from=2026-10-15T10:00:00Z&to=2026-10-15T11:00:00Z&label=mountpoint=/&label=k=a=b",
			want: chstore.SeriesQuery{Target: "t", Name: "m", Step: chstore.Step5m, From: from, To: from.Add(time.Hour),
				Labels: map[string]string{"mountpoint": "/", "k": "a=b"}, MaxPoints: maxPoints},
		},
		{name: "missing name", query: "target=t", wantErr: "required"},
		{name: "bad step", query: "target=t&name=m&step=2m", wantErr: "step"},
		{name: "bad time", query: "target=t&name=m&from=yesterday", wantErr: "from"},
		{name: "empty range", query: "target=t&name=m&from=2026-10-15T12:00:00Z&to=2026-10-15T12:00:00Z", wantErr: "before"},
		{name: "label without value", query: "target=t&name=m&label=mountpoint", wantErr: "name=value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeQuerier{}
			rec := serve(t, newQueryTestHandler(f), http.MethodGet, "/v1/series?"+tt.query)
			if tt.wantErr != "" {
				var body map[string]string
				json.Unmarshal(rec.Body.Bytes(), &body)
				if rec.Code != http.StatusBadRequest || !strings.Contains(body["error"], tt.wantErr) {
					t.Errorf("status %d, body %v; want 400 mentioning %q", rec.Code, body, tt.wantErr)
				}
				return
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			// "now" is taken inside the handler; only the explicit-time case
			// can be compared exactly.
			if tt.name == "defaults" {
				if f.got.To.Sub(f.got.From) != time.Hour || f.got.Step != chstore.StepRaw || f.got.MaxPoints != maxPoints {
					t.Errorf("defaults: %+v", f.got)
				}
				return
			}
			if !reflect.DeepEqual(f.got, tt.want) {
				t.Errorf("got  %+v\nwant %+v", f.got, tt.want)
			}
		})
	}
}

func TestQuerySeriesErrors(t *testing.T) {
	tests := []struct {
		err  error
		want int
	}{
		{chstore.ErrTooManyPoints, http.StatusUnprocessableEntity},
		{errors.New("connection reset"), http.StatusInternalServerError},
	}
	for _, tt := range tests {
		rec := serve(t, newQueryTestHandler(&fakeQuerier{err: tt.err}), http.MethodGet, "/v1/series?target=t&name=m")
		if rec.Code != tt.want {
			t.Errorf("%v: status %d, want %d", tt.err, rec.Code, tt.want)
		}
	}
}

func TestQuerySeriesResponse(t *testing.T) {
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	f := &fakeQuerier{series: []chstore.Series{
		{
			ID: math.MaxUint64, Labels: map[string]string{"mountpoint": "/"}, Type: "gauge",
			Points: []chstore.Point{{Time: at, Value: 1.5}, {Time: at.Add(time.Second), Value: math.NaN()}},
		},
		{
			ID: 7, Type: "gauge",
			Windows: []chstore.Window{{Start: at, Min: 1, Max: math.Inf(1), Avg: 2.5, Last: 3, Count: 4}},
		},
	}}
	rec := serve(t, newQueryTestHandler(f), http.MethodGet, "/v1/series?target=t&name=m")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}

	var body struct {
		Series []map[string]any `json:"series"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body)
	}
	raw, win := body.Series[0], body.Series[1]
	if raw["id"] != "18446744073709551615" {
		t.Errorf("id = %v, want the full uint64 as a string", raw["id"])
	}
	points := raw["points"].([]any)
	if points[0].(map[string]any)["v"] != "1.5" || points[1].(map[string]any)["v"] != "NaN" {
		t.Errorf("points = %v", points)
	}
	if _, has := raw["windows"]; has {
		t.Error("raw series has a windows field")
	}
	w := win["windows"].([]any)[0].(map[string]any)
	if w["max"] != "+Inf" || w["avg"] != "2.5" || w["count"] != float64(4) {
		t.Errorf("window = %v", w)
	}
}

func TestListDetections(t *testing.T) {
	eta := 51*time.Hour + 20*time.Minute + 10*time.Second
	f := &fakeQuerier{detections: []chstore.Detection{
		{Rule: "disk_fill_forecast", Kind: "forecast", Status: "exhausts", Firing: true, Target: "t", SeriesID: 1,
			Labels: map[string]string{"mountpoint": "/"}, Score: math.Inf(-1), Slope: -2, ETA: &eta},
		{Rule: "disk_usage_rate_anomaly", Kind: "anomaly", Status: "normal", Target: "t", SeriesID: 1, Score: 0.5},
	}}
	h := newQueryTestHandler(f)

	before := time.Now()
	rec := serve(t, h, http.MethodGet, "/v1/detections?target=t")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if f.gotTarget != "t" || f.gotSince.After(before.Add(-detectionsFreshness+time.Second)) {
		t.Errorf("target %q since %v, want t and about %v ago", f.gotTarget, f.gotSince, detectionsFreshness)
	}
	var body struct {
		Detections []map[string]any `json:"detections"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Detections) != 2 {
		t.Fatalf("got %d detections", len(body.Detections))
	}
	fc := body.Detections[0]
	if fc["eta"] != "2d3h20m" || fc["eta_seconds"] != eta.Seconds() || fc["score"] != "-Inf" || fc["series_id"] != "1" {
		t.Errorf("forecast = %v", fc)
	}
	if _, has := body.Detections[1]["eta"]; has {
		t.Error("anomaly has an eta")
	}

	rec = serve(t, h, http.MethodGet, "/v1/detections?firing=true")
	json.Unmarshal(rec.Body.Bytes(), &body)
	if len(body.Detections) != 1 || body.Detections[0]["rule"] != "disk_fill_forecast" {
		t.Errorf("firing=true returned %v", body.Detections)
	}
}

func TestFormatETA(t *testing.T) {
	tests := map[time.Duration]string{
		10 * time.Second:                              "now",
		3*time.Hour + 20*time.Minute:                  "3h20m",
		3*time.Hour + 19*time.Minute + 40*time.Second: "3h20m",
		48 * time.Hour:                                "2d",
		25*time.Hour + 5*time.Minute:                  "1d1h5m",
	}
	for d, want := range tests {
		if got := formatETA(d); got != want {
			t.Errorf("formatETA(%v) = %q, want %q", d, got, want)
		}
	}
}

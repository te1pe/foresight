package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/te1pe/foresight/internal/logs"
)

type fakePublisher struct {
	got []logs.Batch
	err error
}

func (f *fakePublisher) PublishLogs(_ context.Context, b logs.Batch) error {
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, b)
	return nil
}

func postLogsRequest(t *testing.T, pub *fakePublisher, query, body string) (*httptest.ResponseRecorder, logsResponse) {
	t.Helper()
	h := NewIngestHandler(fakePinger{}, fakeTargets{}, pub, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/logs"+query, strings.NewReader(body)))
	var resp logsResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec, resp
}

func TestPostLogs(t *testing.T) {
	pub := &fakePublisher{}
	body := `{"time":"2026-10-16T12:00:00Z","level":"ERROR","msg":"apply op","doc":"d1"}
garbage
{"msg":"ws connected"}
`
	rec, resp := postLogsRequest(t, pub, "?service=cloute", body)
	if rec.Code != http.StatusAccepted || resp.Accepted != 2 || resp.Rejected != 1 {
		t.Fatalf("status %d, response %+v", rec.Code, resp)
	}
	if len(pub.got) != 1 || pub.got[0].Service != "cloute" || len(pub.got[0].Entries) != 2 {
		t.Errorf("published %+v", pub.got)
	}
}

func TestPostLogsErrors(t *testing.T) {
	tests := []struct {
		name  string
		query string
		body  string
		err   error
		want  int
	}{
		{"missing service", "", `{"msg":"x"}`, nil, http.StatusBadRequest},
		{"service with spaces", "?service=my%20app", `{"msg":"x"}`, nil, http.StatusBadRequest},
		{"nothing valid", "?service=s", "garbage\n{}\n", nil, http.StatusBadRequest},
		{"body too large", "?service=s", strings.Repeat(`{"msg":"x"}`+"\n", 30000), nil, http.StatusRequestEntityTooLarge},
		{"queue down", "?service=s", `{"msg":"x"}`, errors.New("broker unreachable"), http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{err: tt.err}
			rec, _ := postLogsRequest(t, pub, tt.query, tt.body)
			if rec.Code != tt.want {
				t.Errorf("status %d, want %d: %s", rec.Code, tt.want, rec.Body)
			}
			if tt.want != http.StatusAccepted && len(pub.got) != 0 {
				t.Errorf("published despite status %d", rec.Code)
			}
		})
	}
}

func TestGetLogsNotAllowed(t *testing.T) {
	h := NewIngestHandler(fakePinger{}, fakeTargets{}, &fakePublisher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if rec := serve(t, h, http.MethodGet, "/v1/logs?service=s"); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/logs = %d", rec.Code)
	}
}

package notify

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testToken = "123456:TEST-token-for-unit-tests"

func TestTelegramSend(t *testing.T) {
	var got struct {
		path string
		body sendMessageRequest
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.path = r.URL.Path
		json.NewDecoder(r.Body).Decode(&got.body)
		io.WriteString(w, `{"ok":true,"result":{}}`)
	}))
	defer srv.Close()

	text := "диск / заполнится через 3 ч 20 мин <b>not markup</b>"
	if err := NewTelegram(srv.URL+"/", testToken, "-100").Send(t.Context(), text); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got.path != "/bot"+testToken+"/sendMessage" {
		t.Errorf("path = %q", got.path)
	}
	if got.body.ChatID != "-100" || got.body.Text != text || !got.body.DisableWebPagePreview {
		t.Errorf("body = %+v", got.body)
	}
}

func TestTelegramErrors(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		check   func(error) bool
		wantMsg string
	}{
		{
			name: "flood control", status: http.StatusTooManyRequests,
			body:  `{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 7","parameters":{"retry_after":7}}`,
			check: func(err error) bool { var r *RetryAfterError; return errors.As(err, &r) && r.After == 7*time.Second },
		},
		{
			name: "bad chat", status: http.StatusBadRequest,
			body:    `{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`,
			wantMsg: "chat not found",
		},
		{
			name: "not json", status: http.StatusBadGateway, body: "<html>bad gateway</html>",
			wantMsg: "status 502",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				io.WriteString(w, tt.body)
			}))
			defer srv.Close()

			err := NewTelegram(srv.URL, testToken, "1").Send(t.Context(), "x")
			if err == nil {
				t.Fatal("Send succeeded")
			}
			if tt.check != nil && !tt.check(err) {
				t.Errorf("err = %v", err)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("err = %v, want it to mention %q", err, tt.wantMsg)
			}
			if strings.Contains(err.Error(), testToken) {
				t.Errorf("error leaks the bot token: %v", err)
			}
		})
	}
}

// net/http puts the full request URL into transport errors; for Telegram
// that URL contains the bot token.
func TestTelegramTransportErrorDoesNotLeakToken(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens there anymore

	err := NewTelegram(url, testToken, "1").Send(t.Context(), "x")
	if err == nil {
		t.Fatal("Send to a closed server succeeded")
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), "/bot") {
		t.Errorf("error leaks the request URL: %v", err)
	}
}

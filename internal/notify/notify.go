// Package notify delivers alert messages: to Telegram, or to the log when
// no bot is configured (local development).
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// MaxMessageLen is Telegram's limit for one message, in characters.
const MaxMessageLen = 4096

const sendTimeout = 10 * time.Second

type Telegram struct {
	client  *http.Client
	baseURL string
	token   string
	chatID  string
}

// NewTelegram takes the API base URL so tests can point it at a local
// server instead of api.telegram.org.
func NewTelegram(baseURL, token, chatID string) *Telegram {
	return &Telegram{
		client:  &http.Client{},
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		chatID:  chatID,
	}
}

// RetryAfterError reports Telegram's flood control: nothing may be sent to
// this chat before After has passed.
type RetryAfterError struct {
	After time.Duration
}

func (e *RetryAfterError) Error() string {
	return fmt.Sprintf("telegram rate limit: retry after %s", e.After)
}

type sendMessageRequest struct {
	ChatID                string `json:"chat_id"`
	Text                  string `json:"text"`
	DisableWebPagePreview bool   `json:"disable_web_page_preview"`
}

type apiResponse struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

// Send posts plain text: no parse_mode, so label values with "<", "_" or
// "*" cannot break formatting or be rejected as malformed markup.
func (t *Telegram) Send(ctx context.Context, text string) error {
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()

	body, err := json.Marshal(sendMessageRequest{ChatID: t.chatID, Text: text, DisableWebPagePreview: true})
	if err != nil {
		return fmt.Errorf("telegram: encode message: %w", err)
	}
	// The bot token is part of the URL path; it must never reach an error
	// message or a log line.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+"/bot"+t.token+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		return errors.New("telegram: build request: invalid base URL")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		// *url.Error embeds the full URL, token included: keep only the
		// underlying cause.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return fmt.Errorf("telegram sendMessage: %w", err)
	}
	defer resp.Body.Close()

	var r apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("telegram sendMessage: status %d, unreadable response: %w", resp.StatusCode, err)
	}
	if resp.StatusCode == http.StatusTooManyRequests && r.Parameters.RetryAfter > 0 {
		return &RetryAfterError{After: time.Duration(r.Parameters.RetryAfter) * time.Second}
	}
	if !r.OK {
		return fmt.Errorf("telegram sendMessage: status %d: %s", resp.StatusCode, r.Description)
	}
	return nil
}

// Log stands in for Telegram when no bot is configured, so the whole alert
// pipeline can run locally without credentials.
type Log struct {
	log *slog.Logger
}

func NewLog(log *slog.Logger) *Log { return &Log{log: log} }

func (l *Log) Send(_ context.Context, text string) error {
	l.log.Info("notification (telegram not configured)", "text", text)
	return nil
}

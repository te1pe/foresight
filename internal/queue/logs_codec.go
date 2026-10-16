package queue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/te1pe/foresight/internal/logs"
)

// One record carries one POST /v1/logs request. Raw travels along with the
// parsed fields: storage deduplicates by it, and keeping the original line
// means a parser improvement can be applied to old data.
type wireLogBatch struct {
	Service  string         `json:"service"`
	Received time.Time      `json:"received"`
	Entries  []wireLogEntry `json:"entries"`
}

type wireLogEntry struct {
	Time    time.Time         `json:"time"`
	Level   string            `json:"level,omitempty"`
	Message string            `json:"msg,omitempty"`
	Attrs   map[string]string `json:"attrs,omitempty"`
	Raw     string            `json:"raw"`
}

func EncodeLogs(b logs.Batch) ([]byte, error) {
	w := wireLogBatch{Service: b.Service, Received: b.Received, Entries: make([]wireLogEntry, len(b.Entries))}
	for i, e := range b.Entries {
		w.Entries[i] = wireLogEntry{Time: e.Time, Level: e.Level, Message: e.Message, Attrs: e.Attrs, Raw: e.Raw}
	}
	data, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("encode logs of %s: %w", b.Service, err)
	}
	return data, nil
}

func DecodeLogs(data []byte) (logs.Batch, error) {
	var w wireLogBatch
	if err := json.Unmarshal(data, &w); err != nil {
		return logs.Batch{}, fmt.Errorf("decode logs: %w", err)
	}
	if w.Service == "" {
		return logs.Batch{}, fmt.Errorf("decode logs: missing service")
	}
	b := logs.Batch{Service: w.Service, Received: w.Received, Entries: make([]logs.Entry, len(w.Entries))}
	for i, e := range w.Entries {
		b.Entries[i] = logs.Entry{Time: e.Time, Level: e.Level, Message: e.Message, Attrs: e.Attrs, Raw: e.Raw}
	}
	return b, nil
}

// Package logs parses log lines pushed to POST /v1/logs.
package logs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Entry is one log line. Raw keeps the line exactly as received: it is the
// identity used for deduplication and the ground truth if parsing is ever
// improved.
type Entry struct {
	Time    time.Time
	Level   string
	Message string
	Attrs   map[string]string
	Raw     string
}

// Batch is one request: the service it came from and its entries.
type Batch struct {
	Service  string
	Received time.Time
	Entries  []Entry
}

// MaxLineSize bounds one line; a longer one is rejected, not truncated.
const MaxLineSize = 64 << 10

// Parse reads newline-delimited JSON objects as written by log/slog's JSON
// handler: "time", "level" and "msg" are recognised (and their common
// aliases "ts", "lvl", "message"); every other key becomes an attribute.
// A service can therefore pipe its stdout here unchanged.
//
// A malformed line is skipped and counted, like a malformed metric line:
// one bad line must not cost the rest of the request. A line without a
// usable time gets the receive time.
func Parse(r io.Reader, service string, received time.Time) (b Batch, rejected int, err error) {
	b = Batch{Service: service, Received: received}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 4096), MaxLineSize)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		e, ok := parseLine(line, received)
		if !ok {
			rejected++
			continue
		}
		b.Entries = append(b.Entries, e)
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return b, rejected, fmt.Errorf("a line is longer than %d KiB", MaxLineSize>>10)
		}
		return b, rejected, fmt.Errorf("read log lines: %w", err)
	}
	return b, rejected, nil
}

func parseLine(line []byte, received time.Time) (Entry, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		return Entry{}, false
	}
	e := Entry{Time: received, Raw: string(line), Attrs: map[string]string{}}
	for k, v := range fields {
		switch k {
		case "time", "ts":
			var s string
			if json.Unmarshal(v, &s) == nil {
				if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
					e.Time = t.UTC()
				}
			}
		case "level", "lvl":
			e.Level = strings.ToLower(stringValue(v))
		case "msg", "message":
			e.Message = stringValue(v)
		default:
			e.Attrs[k] = stringValue(v)
		}
	}
	// A line with neither message nor attributes says nothing.
	if e.Message == "" && len(e.Attrs) == 0 {
		return Entry{}, false
	}
	return e, true
}

// stringValue unquotes JSON strings and keeps everything else (numbers,
// booleans, nested objects) as its compact JSON text.
func stringValue(v json.RawMessage) string {
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s
	}
	var buf bytes.Buffer
	if json.Compact(&buf, v) == nil {
		return buf.String()
	}
	return string(v)
}

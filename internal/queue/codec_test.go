package queue

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/te1pe/foresight/internal/scrape"
)

func TestCodecRoundTrip(t *testing.T) {
	in := scrape.Batch{
		Target: "localhost:9100",
		// Nanoseconds survive the trip: idempotent storage relies on a
		// redelivered batch carrying exactly the same time.
		Time: time.Date(2026, 10, 15, 12, 0, 0, 123456789, time.UTC),
		Samples: []scrape.Sample{
			{Name: "node_filesystem_avail_bytes", Labels: map[string]string{"mountpoint": "/", "q": "a \"b\"\n"}, Value: 2.05256060928e+11, Type: scrape.TypeGauge},
			{Name: "tiny", Value: 5e-324, Type: scrape.TypeUntyped},
			{Name: "third", Value: 0.1 + 0.2, Type: scrape.TypeUntyped},
			{Name: "h_bucket", Labels: map[string]string{"le": "+Inf"}, Value: math.Inf(1), Type: scrape.TypeHistogram},
			{Name: "neg", Value: math.Inf(-1), Type: scrape.TypeGauge},
			{Name: "nan", Value: math.NaN(), Type: scrape.TypeGauge},
		},
	}

	data, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if out.Target != in.Target || !out.Time.Equal(in.Time) || len(out.Samples) != len(in.Samples) {
		t.Fatalf("header mismatch: got %s %v %d samples", out.Target, out.Time, len(out.Samples))
	}
	for i, want := range in.Samples {
		got := out.Samples[i]
		sameValue := got.Value == want.Value || (math.IsNaN(got.Value) && math.IsNaN(want.Value))
		if got.Name != want.Name || got.Type != want.Type || !sameValue || len(got.Labels) != len(want.Labels) {
			t.Errorf("sample %d: got %+v, want %+v", i, got, want)
			continue
		}
		for k, v := range want.Labels {
			if got.Labels[k] != v {
				t.Errorf("sample %d label %s = %q, want %q", i, k, got.Labels[k], v)
			}
		}
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	tests := []struct {
		name, data, wantErr string
	}{
		{"not json", "garbage", "decode batch"},
		{"missing target", `{"time":"2026-10-15T12:00:00Z","samples":[]}`, "missing target"},
		{"missing time", `{"target":"t","samples":[]}`, "missing target or time"},
		{"bad value", `{"target":"t","time":"2026-10-15T12:00:00Z","samples":[{"name":"m","value":"abc","type":"gauge"}]}`, "invalid value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode([]byte(tt.data))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

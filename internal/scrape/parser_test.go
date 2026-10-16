package scrape

import (
	"bytes"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestParseSampleLines(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Sample
	}{
		{
			name:  "no labels",
			input: "up 1",
			want:  Sample{Name: "up", Value: 1, Type: TypeUntyped},
		},
		{
			name:  "labels",
			input: `http_requests_total{method="post",code="200"} 1027`,
			want: Sample{Name: "http_requests_total", Value: 1027, Type: TypeUntyped,
				Labels: map[string]string{"method": "post", "code": "200"}},
		},
		{
			name:  "empty label set",
			input: "metric{} 2",
			want:  Sample{Name: "metric", Value: 2, Type: TypeUntyped, Labels: map[string]string{}},
		},
		{
			name:  "trailing comma and spaces inside braces",
			input: `metric{ a = "1" , b="2", } 3`,
			want: Sample{Name: "metric", Value: 3, Type: TypeUntyped,
				Labels: map[string]string{"a": "1", "b": "2"}},
		},
		{
			name:  "escaped backslash quote and newline",
			input: `msdos_file_access_time_seconds{path="C:\\DIR\\FILE.TXT",error="Cannot find file:\n\"FILE.TXT\""} 1.458255915e9`,
			want: Sample{Name: "msdos_file_access_time_seconds", Value: 1.458255915e9, Type: TypeUntyped,
				Labels: map[string]string{"path": `C:\DIR\FILE.TXT`, "error": "Cannot find file:\n\"FILE.TXT\""}},
		},
		{
			name:  "spaces braces and commas inside value",
			input: `info{version="Darwin 25.5.0: {x}, y"} 1`,
			want: Sample{Name: "info", Value: 1, Type: TypeUntyped,
				Labels: map[string]string{"version": "Darwin 25.5.0: {x}, y"}},
		},
		{
			name:  "timestamp",
			input: "metric_without_timestamp_and_labels 12.47 1395066363000",
			want: Sample{Name: "metric_without_timestamp_and_labels", Value: 12.47, Type: TypeUntyped,
				TimestampMs: 1395066363000, HasTimestamp: true},
		},
		{
			name:  "negative timestamp and tabs",
			input: "m\t-3.5\t-1",
			want:  Sample{Name: "m", Value: -3.5, Type: TypeUntyped, TimestampMs: -1, HasTimestamp: true},
		},
		{
			name:  "colon in name",
			input: "job:requests:rate5m 0.5",
			want:  Sample{Name: "job:requests:rate5m", Value: 0.5, Type: TypeUntyped},
		},
		{
			name:  "positive infinity",
			input: `h_bucket{le="+Inf"} +Inf`,
			want: Sample{Name: "h_bucket", Value: math.Inf(1), Type: TypeUntyped,
				Labels: map[string]string{"le": "+Inf"}},
		},
		{
			name:  "negative infinity",
			input: "m -Inf",
			want:  Sample{Name: "m", Value: math.Inf(-1), Type: TypeUntyped},
		},
		{
			name:  "exponent",
			input: "m 2.0525647872e+11",
			want:  Sample{Name: "m", Value: 2.0525647872e+11, Type: TypeUntyped},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := Parse(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if res.Skipped != 0 {
				t.Fatalf("Skipped = %d, FirstErr = %v", res.Skipped, res.FirstErr)
			}
			if len(res.Samples) != 1 {
				t.Fatalf("got %d samples, want 1", len(res.Samples))
			}
			if got := res.Samples[0]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

// NaN is checked separately because NaN != NaN breaks reflect.DeepEqual.
func TestParseNaN(t *testing.T) {
	res, err := Parse(strings.NewReader("m NaN"))
	if err != nil || len(res.Samples) != 1 {
		t.Fatalf("Parse: %v, samples=%d", err, len(res.Samples))
	}
	if !math.IsNaN(res.Samples[0].Value) {
		t.Errorf("Value = %v, want NaN", res.Samples[0].Value)
	}
}

func TestParseMalformedLines(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantErr string
	}{
		{"missing value", "metric", "expected whitespace"},
		{"missing value after labels", `metric{a="1"}`, "expected whitespace"},
		{"bad value", "metric abc", "invalid value"},
		{"bad timestamp", "metric 1 abc", "invalid timestamp"},
		{"float timestamp", "metric 1 1.5", "invalid timestamp"},
		{"trailing garbage", "metric 1 2 3", "trailing data"},
		{"name starts with digit", "1metric 1", "invalid metric name"},
		{"unquoted label value", "metric{a=1} 1", "double-quoted"},
		{"unterminated label value", `metric{a="1} 1`, "unterminated value"},
		{"unclosed label set", `metric{a="1" 1`, "expected ',' or '}'"},
		{"unknown escape", `metric{a="\t"} 1`, "unknown escape"},
		{"missing equals", `metric{a"1"} 1`, "expected '='"},
		{"empty label name", `metric{="1"} 1`, "invalid label name"},
		{"duplicate label", `metric{a="1",a="2"} 1`, "duplicate label"},
		{"unknown type", "# TYPE metric gaugeish", "unknown metric type"},
		{"short TYPE line", "# TYPE metric", "TYPE line"},
		{"invalid name in TYPE", "# TYPE me-tric gauge", "invalid metric name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := Parse(strings.NewReader(tt.line))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if res.Skipped != 1 || len(res.Samples) != 0 {
				t.Fatalf("Skipped = %d, samples = %d; want 1 and 0", res.Skipped, len(res.Samples))
			}
			if res.FirstErr == nil || !strings.Contains(res.FirstErr.Error(), tt.wantErr) {
				t.Errorf("FirstErr = %v, want it to contain %q", res.FirstErr, tt.wantErr)
			}
		})
	}
}

func TestParseSkipsBadLinesAndKeepsRest(t *testing.T) {
	input := strings.Join([]string{
		"# HELP good_total A counter.",
		"# TYPE good_total counter",
		"good_total 1",
		"broken{ 5",
		"",
		"good_total{x=\"y\"} 2",
		"also broken",
		"# a free-form comment is not an error",
	}, "\n")

	res, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Samples) != 2 {
		t.Errorf("got %d samples, want 2", len(res.Samples))
	}
	if res.Skipped != 2 {
		t.Errorf("Skipped = %d, want 2", res.Skipped)
	}
	if res.FirstErr == nil || !strings.HasPrefix(res.FirstErr.Error(), "line 4:") {
		t.Errorf("FirstErr = %v, want it to point at line 4", res.FirstErr)
	}
}

func TestParseTypes(t *testing.T) {
	input := `# TYPE c counter
c 1
# TYPE g gauge
g 2
# TYPE s summary
s{quantile="0.5"} 3
s_sum 4
s_count 5
# TYPE h histogram
h_bucket{le="1"} 6
h_bucket{le="+Inf"} 7
h_sum 8
h_count 9
# TYPE u untyped
u 10
not_declared 11
s_bucket 12
`
	want := map[string]MetricType{
		"c": TypeCounter, "g": TypeGauge,
		"s": TypeSummary, "s_sum": TypeSummary, "s_count": TypeSummary,
		"h_bucket": TypeHistogram, "h_sum": TypeHistogram, "h_count": TypeHistogram,
		"u": TypeUntyped, "not_declared": TypeUntyped,
		// Summaries have no buckets, so this is not part of the s family.
		"s_bucket": TypeUntyped,
	}

	res, err := Parse(strings.NewReader(input))
	if err != nil || res.Skipped != 0 {
		t.Fatalf("Parse: err=%v skipped=%d first=%v", err, res.Skipped, res.FirstErr)
	}
	for _, s := range res.Samples {
		if s.Type != want[s.Name] {
			t.Errorf("%s: type %q, want %q", s.Name, s.Type, want[s.Name])
		}
	}
}

func TestParseNodeExporterFixture(t *testing.T) {
	f, err := os.Open("../../testdata/node_exporter.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	res, err := Parse(f)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if res.Skipped != 0 {
		t.Fatalf("Skipped = %d, FirstErr = %v", res.Skipped, res.FirstErr)
	}
	// Count of non-comment, non-empty lines in the fixture.
	if len(res.Samples) != 538 {
		t.Errorf("got %d samples, want 538", len(res.Samples))
	}

	var avail, uname, gcQuantiles int
	for _, s := range res.Samples {
		if s.Type == TypeUntyped {
			t.Errorf("%s: every family in the fixture declares a TYPE, got untyped", s.Name)
		}
		switch s.Name {
		case "node_filesystem_avail_bytes":
			avail++
			if s.Type != TypeGauge || s.Labels["mountpoint"] == "" {
				t.Errorf("node_filesystem_avail_bytes: type %q, labels %v", s.Type, s.Labels)
			}
		case "node_uname_info":
			uname++
			if !strings.Contains(s.Labels["version"], "Darwin Kernel Version") {
				t.Errorf("node_uname_info version label = %q", s.Labels["version"])
			}
		case "go_gc_duration_seconds":
			gcQuantiles++
			if s.Type != TypeSummary {
				t.Errorf("go_gc_duration_seconds: type %q, want summary", s.Type)
			}
		}
	}
	if avail == 0 || uname != 1 || gcQuantiles != 5 {
		t.Errorf("avail=%d uname=%d gcQuantiles=%d", avail, uname, gcQuantiles)
	}
}

// BenchmarkParseNodeExporter parses the real fixture: one scrape of
// node_exporter on macOS, 752 lines, 538 samples.
func BenchmarkParseNodeExporter(b *testing.B) {
	data, err := os.ReadFile("../../testdata/node_exporter.txt")
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		res, err := Parse(bytes.NewReader(data))
		if err != nil || len(res.Samples) != 538 {
			b.Fatalf("Parse: %v, %d samples", err, len(res.Samples))
		}
	}
}

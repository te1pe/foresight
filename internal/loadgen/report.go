package loadgen

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// Report holds what one run measured. Rates are per second over the
// measurement window; resource figures come from the process collectors of
// the services themselves.
type Report struct {
	Targets  int
	Window   time.Duration
	LogsSent int64
	// LogsFailed counts lines whose request did not get a 202.
	LogsFailed int64

	ScrapesPerSec        float64
	SamplesScrapedPerSec float64
	ScrapeFailures       float64
	ScrapeP99            float64
	PublishErrors        float64

	RowsStoredPerSec float64
	InsertP99        float64
	HandleRetries    float64
	Dropped          float64
	LatencyP50       float64
	LatencyP90       float64
	LatencyP99       float64
	LogsStoredPerSec float64
	LogLatencyP99    float64

	IngestCPU, ProcessorCPU             float64 // cores
	IngestRSS, ProcessorRSS             float64 // bytes
	IngestGoroutines, ProcGoroutines    float64
	IngestHeapInuse, ProcessorHeapInuse float64
}

func buildReport(cfg Config, before, after readings) Report {
	secs := after.ingest.at.Sub(before.ingest.at).Seconds()
	psecs := after.processor.at.Sub(before.processor.at).Seconds()
	ing := func(name string, match map[string]string) float64 {
		return after.ingest.sum(name, match) - before.ingest.sum(name, match)
	}
	proc := func(name string, match map[string]string) float64 {
		return after.processor.sum(name, match) - before.processor.sum(name, match)
	}
	hist := func(a, b snapshot, name string, match map[string]string) []bucket {
		return delta(a.buckets(name, match), b.buckets(name, match))
	}

	samples := map[string]string{"kind": "samples"}
	logsKind := map[string]string{"kind": "logs"}
	latency := hist(after.processor, before.processor, "foresight_pipeline_latency_seconds", samples)

	return Report{
		Targets: cfg.Targets,
		Window:  time.Duration(secs * float64(time.Second)).Round(time.Second),

		ScrapesPerSec:        ing("foresight_scrape_duration_seconds_count", nil) / secs,
		SamplesScrapedPerSec: ing("foresight_scrape_samples_total", nil) / secs,
		ScrapeFailures:       ing("foresight_scrape_failures_total", nil),
		ScrapeP99:            quantile(0.99, hist(after.ingest, before.ingest, "foresight_scrape_duration_seconds", nil)),
		PublishErrors:        ing("foresight_queue_published_total", map[string]string{"result": "error"}),

		RowsStoredPerSec: proc("foresight_clickhouse_inserted_rows_total", map[string]string{"table": "samples"}) / psecs,
		InsertP99:        quantile(0.99, hist(after.processor, before.processor, "foresight_clickhouse_insert_duration_seconds", map[string]string{"table": "samples"})),
		HandleRetries:    proc("foresight_queue_handle_retries_total", nil),
		Dropped:          proc("foresight_queue_dropped_total", nil),
		LatencyP50:       quantile(0.50, latency),
		LatencyP90:       quantile(0.90, latency),
		LatencyP99:       quantile(0.99, latency),
		LogsStoredPerSec: proc("foresight_clickhouse_inserted_rows_total", map[string]string{"table": "logs"}) / psecs,
		LogLatencyP99:    quantile(0.99, hist(after.processor, before.processor, "foresight_pipeline_latency_seconds", logsKind)),

		IngestCPU:          ing("process_cpu_seconds_total", nil) / secs,
		ProcessorCPU:       proc("process_cpu_seconds_total", nil) / psecs,
		IngestRSS:          after.ingest.sum("process_resident_memory_bytes", nil),
		ProcessorRSS:       after.processor.sum("process_resident_memory_bytes", nil),
		IngestGoroutines:   after.ingest.sum("go_goroutines", nil),
		ProcGoroutines:     after.processor.sum("go_goroutines", nil),
		IngestHeapInuse:    after.ingest.sum("go_memstats_heap_inuse_bytes", nil),
		ProcessorHeapInuse: after.processor.sum("go_memstats_heap_inuse_bytes", nil),
	}
}

func (r Report) String() string {
	var b strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	line("targets %d, window %s", r.Targets, r.Window)
	line("ingest     scrapes %.1f/s, samples %.0f/s, scrape p99 %s, failures %.0f, publish errors %.0f",
		r.ScrapesPerSec, r.SamplesScrapedPerSec, dur(r.ScrapeP99), r.ScrapeFailures, r.PublishErrors)
	line("processor  rows stored %.0f/s, insert p99 %s, retries %.0f, dropped %.0f",
		r.RowsStoredPerSec, dur(r.InsertP99), r.HandleRetries, r.Dropped)
	line("latency    scrape → stored  p50 %s  p90 %s  p99 %s", dur(r.LatencyP50), dur(r.LatencyP90), dur(r.LatencyP99))
	if r.LogsSent > 0 || r.LogsFailed > 0 {
		line("logs       sent %.0f/s (failed %d), stored %.0f/s, receive → stored p99 %s",
			float64(r.LogsSent)/r.Window.Seconds(), r.LogsFailed, r.LogsStoredPerSec, dur(r.LogLatencyP99))
	}
	line("resources  ingest %.2f cores, RSS %s, heap %s, %0.f goroutines",
		r.IngestCPU, mib(r.IngestRSS), mib(r.IngestHeapInuse), r.IngestGoroutines)
	line("           processor %.2f cores, RSS %s, heap %s, %0.f goroutines",
		r.ProcessorCPU, mib(r.ProcessorRSS), mib(r.ProcessorHeapInuse), r.ProcGoroutines)
	return b.String()
}

func dur(seconds float64) string {
	if math.IsNaN(seconds) {
		return "n/a"
	}
	return time.Duration(seconds * float64(time.Second)).Round(time.Millisecond).String()
}

func mib(bytes float64) string {
	if bytes == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.0f MiB", bytes/(1<<20))
}

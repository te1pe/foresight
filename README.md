# foresight

foresight collects metrics and logs from services, looks for anomalies and
**forecasts resource exhaustion before it causes an outage**: "the disk will be
full in 3 h 20 min", "the connection pool will run out in 4 h".

- `ingest` scrapes the targets listed in the `targets` table (PostgreSQL),
  parses the Prometheus text format with its own parser, accepts logs through
  `POST /v1/logs` and publishes everything to Redpanda;
- `processor` reads them in a consumer group and writes them to ClickHouse
  idempotently. 1m/5m windows are computed by materialized views on insert;
- once a minute `processor` runs detectors over the windows: an EWMA z-score on
  the rate of change, an exhaustion forecast (disk) and sustained growth
  (a goroutine leak, a CRDT journal without compaction);
- `query` serves series and detector verdicts over HTTP: "`/` will be full in
  1d21h";
- `alerter` turns detections into incidents (PostgreSQL) and sends Telegram
  notifications without duplicates (Redis).

At-least-once delivery, duplicate-free storage: redelivery changes neither the
raw points nor the aggregates. foresight watches itself: every service exposes
`/metrics` and pprof, and foresight scrapes itself. Load-test numbers are in
[Load testing](#load-testing).

The [Cloute](https://github.com/te1pe/cloute) integration is done on
foresight's side (targets, log intake, the CRDT journal rule); Cloute itself
does not export the journal metric yet.

## Architecture

Green marks what works, grey what is not connected yet.

```mermaid
flowchart TD
    NE[node_exporter<br/>localhost:9100]
    CL[Cloute /metrics]
    LG[POST /v1/logs]
    IN[cmd/ingest<br/>scrape + log intake]
    RP[[Redpanda<br/>topic foresight.samples]]
    PR[cmd/processor<br/>consumer group,<br/>idempotent writes]
    CH[(ClickHouse<br/>samples → MV → samples_1m / samples_5m<br/>detections)]
    QY[cmd/query<br/>/v1/series, /v1/detections]
    DT[processor: rules once a minute<br/>EWMA z-score, least-squares forecast]
    AL[cmd/alerter<br/>incidents + outbox]
    PG[(PostgreSQL<br/>incidents, incident_events)]
    RD[(Redis)]
    TG[Telegram]

    NE --> IN
    CL -.-> IN
    LG --> IN
    IN --> RP
    RP --> PR
    PR --> CH
    CH --> QY
    CH --> DT
    DT --> CH
    CH --> AL
    AL --> PG
    AL --> RD
    AL --> TG

    classDef done fill:#d4edda,stroke:#28a745,color:#000
    classDef todo fill:#f1f1f1,stroke:#999,color:#666,stroke-dasharray: 4 3
    class NE,IN,RP,PR,CH,QY,DT,AL,PG,RD,TG done
    class CL todo
```

PostgreSQL holds the metadata: incidents, their notification queue and the
targets table.

## How it works

### ingest

- Targets live in the `targets` table and are re-read every 30 s. Adding and
  disabling a target works without a restart:
  ```sql
  INSERT INTO targets (target) VALUES ('localhost:8080');          -- e.g. Cloute
  UPDATE targets SET enabled = false WHERE target = 'localhost:8080';
  ```
  If PostgreSQL is unavailable, the last known list keeps being scraped.
- One goroutine per target. The first scrape happens at startup, then every
  `FORESIGHT_SCRAPE_INTERVAL`, with 10 s per request. All samples of one scrape
  get one timestamp. Synthetic `up` and `scrape_duration_seconds` are added, as
  in Prometheus.
- One scrape of one target is one Kafka record (JSON, values as strings) keyed
  by `target`.
- The producer is idempotent, `acks=all`, with the buffer capped at 64 MiB. If
  the broker is down, scrapers slow down (backpressure) and no data is lost.
- `GET /healthz`, `GET /readyz` (checks the broker), `GET /v1/targets` (target
  state).
- `POST /v1/logs?service=NAME` accepts NDJSON, e.g. the output of
  `slog.NewJSONHandler` as is:
  ```bash
  ./my-service 2>&1 | curl --data-binary @- 'localhost:8080/v1/logs?service=my-service'
  ```
  The `202 {"accepted":N,"rejected":M}` response comes only after the broker
  acknowledges; `503` means "retry". Malformed lines are rejected one by one.
  Limits: body up to 256 KiB (otherwise 413), line up to 64 KiB. In ClickHouse
  logs go to the `logs` table; duplicates (a retried request, redelivery)
  collapse by `(service, ts, line hash)`, so identical lines without their own
  timestamp merge into one.

### processor

- Consumer group `foresight-processor`. Offsets are committed only after a
  successful write. Transient errors are retried with a 0.5–10 s pause and no
  commit.
- **One ClickHouse insert per scrape**, with
  `insert_deduplication_token = target|scrape time`. A redelivered scrape
  carries the same token and ClickHouse drops it. How the consumer grouped
  records into a poll does not matter.
- `deduplicate_blocks_in_dependent_materialized_views=1` is mandatory: without
  it ClickHouse 26.3 drops the duplicate in the raw table but still passes it to
  the materialized views, and `count`/`avg` in the windows are inflated. This
  was verified on the server and is pinned by an integration test.

### ClickHouse

| Table | Engine | Sort key | Retention |
|---|---|---|---|
| `samples` | ReplacingMergeTree | `(target, name, series_id, ts)` | 14 days |
| `samples_1m` | AggregatingMergeTree, filled by an MV | `(target, name, series_id, window_start)` | 90 days |
| `samples_5m` | AggregatingMergeTree, filled by an MV | `(target, name, series_id, window_start)` | 365 days |

- `series_id` is an FNV-1a hash of `(target, name, sorted labels)`: a `Map`
  cannot be part of a sort key.
- Windows store `min`, `max`, `count` (simple aggregates) and the states of
  `avg` and `last = argMax(value, ts)`, which are merged on read with `-Merge`.
- Deduplication has two levels. The token protects against repeats within the
  last 10,000 inserts. ReplacingMergeTree plus reading raw points with `FINAL`
  is the safety net for raw data against older repeats.

### Detectors

Once a minute, 20 s after the boundary (so the last window has time to be
written), processor evaluates the rules over the closed 1m windows of the last
hour. Every result, firing or not, is written to `detections`.

| Rule | What it does | Fires on |
|---|---|---|
| `disk_fill_forecast` | least squares over `node_filesystem_avail_bytes` → time to zero | a significant decline (t ≤ −3) and ETA ≤ 24 h |
| `disk_usage_rate_anomaly` | EWMA z-score of the rate of change of free space (α=0.1, \|z\| ≥ 4) | a sharp spike in the rate |
| `goroutine_leak` | sustained growth of `go_goroutines` | t ≥ 3, growth ≥ 30 min, +20% and +50 |
| `crdt_journal_growth` | sustained growth of `cloute_crdt_journal_size` without compaction | t ≥ 3, ≥ 45 min without a drop, +50% and +1000 |

`go_goroutines` is exposed by any Go service with the Prometheus client.
`cloute_crdt_journal_size` is a contract with Cloute: a gauge with the number of
operations in a document's journal. Compaction (a sharp drop) resets the growth
count, so the normal sawtooth does not fire (there is a test).

- **The forecast looks only at points after the last jump in the rate.** A
  one-off 50 GB copy is a step, not a trend. Without this the regression would
  say "full in 2 hours" (there is a control test). The same trick finds where
  filling started in the middle of the window.
- **Trend significance is checked with the t-statistic**, not R²: on pure noise,
  0 false trends out of 300 runs.
- **Limitation: seasonality.** Over a window of a couple of periods a sine wave
  produces a false slope (t = −4.6 on synthetic data). The magnitude saves it:
  ETA ≈ 9 days, beyond the horizon. An hour-long window does not see daily
  seasonality.
- Evaluation is a pure function of the data and the minute. A repeat or a second
  processor produce the same keys, and ReplacingMergeTree collapses them.

The detectors are tested on synthetic data: trend, noise, step, spike,
seasonality and their combinations (`internal/detect`).

### alerter

Every 15 s alerter reads fresh evaluations from `detections` and maintains
incidents:

```
firing 2 rounds in a row ──► incident opened ──► "opened" event in the outbox
quiet 3 rounds in a row  ──► incident closed ──► "resolved" event
series not evaluated for 10 min (while others are) ──► closed as no_data
```

- **Hysteresis** (2 to open, 3 to close) keeps an incident from flapping at the
  threshold.
- **One open incident per series** is guaranteed by a partial unique index in
  PostgreSQL, not by code. Exactly one of two racing alerters wins.
- **Transactional outbox:** the notification is written in the same transaction
  as the transition. A crash between them loses nothing.
- **A Redis claim before sending:** `pending:<token>` for 2 min (SET NX), then
  `sent` for 7 days after success. Retries and parallel alerters do not send the
  same thing twice. Release atomically compares the token (Lua) so it never
  deletes someone else's claim.
- Transitions of one round go out as **one message**. A 429 from Telegram pauses
  delivery for `retry_after`.
- Without `FORESIGHT_TELEGRAM_TOKEN` notifications go to the log: the whole
  pipeline works locally without a bot.

A notification (the text is in Russian):

```
🔴 Диск заполняется
localhost:9100 mountpoint=/
заполнится через 3 ч 20 мин (свободно 12.0 ГиБ, убывает на 60.0 МиБ/мин)
```

### query

```
GET /v1/series?target=localhost:9100&name=node_filesystem_avail_bytes
              &step=raw|1m|5m&from=RFC3339&to=RFC3339&label=mountpoint=/
```

- Defaults are `step=raw` and the last hour. `label` can be repeated and is an
  exact match.
- Values are returned as strings (`"NaN"` and `"+Inf"` are allowed), and so is
  the series `id`: a uint64 does not fit into a JavaScript number.
- More than 50,000 points returns 422 with a hint to narrow the query. A
  truncated series would look like real data.

```
GET /v1/detections[?target=localhost:9100][&firing=true]
```

The latest verdict of every rule for every series over 15 minutes: `status`,
`firing`, `score` (t or z), `eta` ("1d21h34m") and `eta_seconds`.
- A separate service, so a heavy query does not slow down intake and
  processing.

## Self-monitoring

Every service has an admin server on loopback only, with `/metrics`
(client_golang) and `/debug/pprof/*`:

| Service | Address |
|---|---|
| ingest | `127.0.0.1:6060` |
| processor | `127.0.0.1:6061` |
| query | `127.0.0.1:6062` |
| alerter | `127.0.0.1:6063` |

pprof exposes stacks and the heap, so it is kept off the public ports (there is
a test). Migration `00005` adds these addresses to `targets`: foresight scrapes
itself, and its `goroutine_leak` rule watches its own processes.

Key metrics:

| Metric | What it shows |
|---|---|
| `foresight_pipeline_latency_seconds{kind}` | time from the scrape (or log intake) to the ClickHouse write; the main number under load |
| `foresight_scrape_duration_seconds{target}`, `_samples_total`, `_failures_total` | target scraping |
| `foresight_queue_handle_duration_seconds`, `_handle_retries_total`, `_dropped_total` | consumer: handling, retries, dropped |
| `foresight_clickhouse_insert_duration_seconds{table}`, `_inserted_rows_total` | writes |
| `foresight_rules_firing{rule}`, `foresight_alert_open_incidents` | detectors and incidents |
| `foresight_http_request_duration_seconds{handler}` | API; the label is the route pattern, not the raw path |

```bash
go tool pprof -top 'http://127.0.0.1:6061/debug/pprof/profile?seconds=30'
```

## Load testing

`make load` (`cmd/loadgen`) starts a fake exporter that serves a real
node_exporter fixture (540 samples) on `/t/{i}/metrics`. It registers N such
targets in `targets` (and removes them on exit), can send logs, and at the end
scrapes foresight's `/metrics` with our own parser and computes quantiles from
the histogram buckets, just like `histogram_quantile`. Parameters:
`FORESIGHT_LOADGEN_TARGETS`, `_DURATION`, `_WARMUP`, `_LOGS_RPS`.

Setup: MacBook Air M3, 8 cores, 16 GB; PostgreSQL, Redpanda, ClickHouse and
Redis in OrbStack on the same machine. Scrape interval 15 s, a 90 s measurement
window after warm-up.

| Targets | Samples/s | Rows written/s | Latency p50 / p90 / p99 | CPU ingest / processor | RSS ingest / processor |
|---|---|---|---|---|---|
| 100 | 3,628 | 3,635 | 113 ms / 239 ms / 660 ms | 0.03 / 0.03 cores | 27 / 27 MiB |
| 150 | 5,415 | 5,435 | 147 ms / 452 ms / 876 ms | 0.04 / 0.04 | 26 / 33 MiB |
| 200 | 7,208 | 7,235 | 322 ms / 854 ms / 1.66 s | 0.05 / 0.05 | 26 / 32 MiB |
| **300** | 10,799 | **7,720** | 20 s / 39 s / 44 s, growing | 0.07 / 0.04 | 28 / 128 MiB |
| 150 + 2,000 log lines/s | 5,420 | 5,435 + 2,000 logs | 142 ms / 454 ms / 877 ms; logs p99 128 ms | 0.08 / 0.07 | 25 / 31 MiB |

**The ceiling is about 7.7k rows/s, i.e. roughly 14 inserts/s or ~210 targets
of this size at a 15 s interval.** At 300 targets processor falls behind: lag
grew to 872 records and was cleared 91 s after the run, nothing was lost. The
bottleneck is not CPU (both services use less than 0.1 core, CPU profiles under
load show ~6% of one core) but **sequential "one per scrape" inserts**: about
70 ms each waiting on ClickHouse.

What the load test found and what changed:

| Finding | Before | After |
|---|---|---|
| All targets added in one reconciliation were scraped in the same second, and every 15 s processor got a burst of inserts | p99 11.6 s with insert p99 of 140 ms | a deterministic phase offset by target hash, as in Prometheus: **p99 660 ms** |
| The default `http.Transport` keeps 2 keep-alive connections per `host:port`: exporters behind one address reconnected on every scrape | scrape p99 3.2 s, errors | a 256-connection pool: scrape p99 5–20 ms |
| `consumer.Close()` with the broker down waited forever to leave the group, past the shutdown timeout | the process did not exit on SIGTERM | leaving the group is bounded by the same timeout |
| An attempt to parallelise inserts (8 workers) | — | **rolled back**: under load on a live database ClickHouse 26.3 failed concurrent inserts into the 1m MV with `LOGICAL_ERROR: Invalid partition key size: 0`, while the same records went through sequentially. The error did not reproduce in a clean container. A state-dependent server error that wedges a partition is worse than a known ceiling. |

How to raise the ceiling: fewer, larger inserts. That needs a deduplication key
that survives regrouping scrapes into inserts (today the token is one scrape) —
for example deterministic slicing by `(Kafka partition, offset range)` or
deduplication before the MV. It changes the delivery guarantees, so it is a
separate task.

The custom text-format parser: **0.2 ms** on real node_exporter output
(752 lines, 538 samples), about 258 MB/s, 3,125 allocations
(`go test -bench ParseNodeExporter ./internal/scrape/`).

## Configuration

| Variable | Default | Read by |
|---|---|---|
| `FORESIGHT_SCRAPE_INTERVAL` | `15s` | ingest |
| `FORESIGHT_HTTP_ADDR` | `:8080` | ingest |
| `FORESIGHT_KAFKA_BROKERS` | `localhost:19092` | ingest, processor |
| `FORESIGHT_KAFKA_TOPIC` | `foresight.samples` | ingest, processor |
| `FORESIGHT_KAFKA_LOGS_TOPIC` | `foresight.logs` | ingest, processor |
| `FORESIGHT_KAFKA_GROUP` | `foresight-processor` | processor |
| `FORESIGHT_CLICKHOUSE_DSN` | `clickhouse://foresight:foresight@localhost:9000/foresight` | processor, query |
| `FORESIGHT_QUERY_HTTP_ADDR` | `:8081` | query |
| `FORESIGHT_DB_DSN` | `postgres://foresight:foresight@localhost:5433/foresight?sslmode=disable` | ingest (targets), alerter |
| `FORESIGHT_REDIS_ADDR` | `localhost:6379` | alerter |
| `FORESIGHT_TELEGRAM_TOKEN` | — (empty: notifications go to the log) | alerter |
| `FORESIGHT_TELEGRAM_CHAT_ID` | — (required together with the token) | alerter |
| `FORESIGHT_TELEGRAM_API_URL` | `https://api.telegram.org` | alerter |
| `FORESIGHT_ADMIN_ADDR` | per service, `127.0.0.1:6060`–`6063` | all |

## Running on macOS

You need Go (the latest stable), Docker (Docker Desktop or OrbStack) and
Homebrew.

1. Run `node_exporter` **natively**, not in Docker. On macOS containers run
   inside a Linux VM, so from Docker you would get the VM's metrics, not the
   Mac's.
   ```bash
   brew install node_exporter
   brew services start node_exporter
   ```
2. Start PostgreSQL (5433), Redpanda (19092), ClickHouse (9000 native, 8123
   HTTP) and Redis (6379). The command creates the topic too.
   ```bash
   make compose-up
   ```
3. Apply the migrations for both databases. goose runs through `go run` with a
   pinned version.
   ```bash
   make migrate-up
   ```
4. In four terminals:
   ```bash
   make run-processor
   ```
   ```bash
   make run-query
   ```
   ```bash
   make run-alerter
   ```
   ```bash
   make run
   ```
   For Telegram, create a bot with @BotFather, find your `chat_id` and before
   `make run-alerter` run
   `export FORESIGHT_TELEGRAM_TOKEN=... FORESIGHT_TELEGRAM_CHAT_ID=...`. Keep the
   token in the environment and never commit it.
5. Check (the forecast needs at least 10 minutes of data):
   ```bash
   curl -s 'localhost:8081/v1/series?target=localhost:9100&name=node_filesystem_avail_bytes&step=1m&label=mountpoint=/'
   curl -s 'localhost:8081/v1/detections?target=localhost:9100'
   ```

Stopping: `Ctrl+C` or `kill -TERM <pid>`. The infrastructure is shut down with
`make compose-down`; the data stays in the volumes.

Since Kafka keeps history, a new store can be filled from the topic: stop
processor, run `rpk group seek foresight-processor --to start` and start it
again. Whatever is already written is dropped by deduplication.

## Development

```bash
make lint   # go vet + gofmt, prints nothing on success
make test   # all tests with -race; integration tests need Docker running
```

Integration tests start PostgreSQL, Redpanda, ClickHouse and Redis through
testcontainers-go and apply the real migrations. With `go test -short ./...`
they are skipped.

## Project layout

```
cmd/ingest/                  # scraping and log intake → Redpanda, /v1/targets, /v1/logs
cmd/processor/               # Redpanda → ClickHouse (samples and logs), rule evaluation every minute
cmd/query/                   # GET /v1/series, /v1/detections
cmd/alerter/                 # detections → incidents → Telegram
cmd/loadgen/                 # load test (make load)
internal/config/             # reading FORESIGHT_* from the environment
internal/scrape/             # Prometheus text format parser, scraper, targets watcher
internal/logs/               # NDJSON log parsing
internal/queue/              # message format, producer, at-least-once consumer
internal/detect/             # EWMA z-score, least-squares forecast, sustained growth (pure maths)
internal/rules/              # disk and Go service rules, the evaluation loop
internal/alert/              # incident lifecycle, outbox delivery
internal/notify/             # Telegram and log notifiers
internal/storage/redis/      # claims for send deduplication
internal/storage/clickhouse/ # samples, windows, detections, logs
internal/storage/postgres/   # targets, incidents and the outbox; the original PostgreSQL sample writer (kept, unused)
internal/httpapi/            # ingest and query handlers, HTTP metrics
internal/admin/              # /metrics and pprof on loopback
internal/loadgen/            # fake exporter, reading /metrics, the report
migrations/                  # goose SQL for PostgreSQL
migrations/clickhouse/       # goose SQL for ClickHouse
deploy/docker-compose.yml    # PostgreSQL 18, Redpanda 26.2, ClickHouse 26.3, Redis 8.8
testdata/                    # real node_exporter output
```

Документация на русском: [README.ru.md](README.ru.md).

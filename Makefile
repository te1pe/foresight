GOOSE_VERSION    := v3.29.0
COMPOSE          := docker compose -f deploy/docker-compose.yml
FORESIGHT_DB_DSN ?= postgres://foresight:foresight@localhost:5433/foresight?sslmode=disable
FORESIGHT_CLICKHOUSE_DSN ?= clickhouse://foresight:foresight@localhost:9000/foresight
TOPIC            := foresight.samples
LOGS_TOPIC       := foresight.logs
# One partition per possible processor instance: the partition count caps
# how far the consumer group can scale out.
TOPIC_PARTITIONS := 3
RPK              := $(COMPOSE) exec -T redpanda rpk

# goose runs via "go run" pinned to the same version as go.mod, so nothing is
# installed system-wide. The tags drop drivers for databases we do not use.
GOOSE := go run -tags="no_libsql no_mssql no_mysql no_sqlite3 no_vertica no_ydb" github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)

.PHONY: compose-up compose-down topic-create migrate-up migrate-status build run run-processor run-query run-alerter load test lint

compose-up:
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory topic-create

topic-create:
	$(RPK) topic create $(TOPIC) $(LOGS_TOPIC) --partitions $(TOPIC_PARTITIONS) --replicas 1 --if-not-exists

compose-down:
	$(COMPOSE) down

# PostgreSQL migrations live in migrations/ (already applied there, so they
# stay put); ClickHouse has its own directory and its own version table.
migrate-up:
	$(GOOSE) -dir migrations postgres "$(FORESIGHT_DB_DSN)" up
	$(GOOSE) -dir migrations/clickhouse clickhouse "$(FORESIGHT_CLICKHOUSE_DSN)" up

migrate-status:
	$(GOOSE) -dir migrations postgres "$(FORESIGHT_DB_DSN)" status
	$(GOOSE) -dir migrations/clickhouse clickhouse "$(FORESIGHT_CLICKHOUSE_DSN)" status

build:
	go build -o bin/ingest ./cmd/ingest
	go build -o bin/processor ./cmd/processor
	go build -o bin/query ./cmd/query
	go build -o bin/alerter ./cmd/alerter
	go build -o bin/loadgen ./cmd/loadgen

# A built binary instead of "go run": the PID in "ps" is the service itself,
# so "kill -TERM <pid>" reaches the process that handles graceful shutdown.
run: build
	FORESIGHT_DB_DSN="$(FORESIGHT_DB_DSN)" exec ./bin/ingest

run-processor: build
	FORESIGHT_CLICKHOUSE_DSN="$(FORESIGHT_CLICKHOUSE_DSN)" exec ./bin/processor

run-query: build
	FORESIGHT_CLICKHOUSE_DSN="$(FORESIGHT_CLICKHOUSE_DSN)" exec ./bin/query

# Telegram is optional: without FORESIGHT_TELEGRAM_TOKEN and
# FORESIGHT_TELEGRAM_CHAT_ID in the environment, alerts go to the log.
run-alerter: build
	FORESIGHT_DB_DSN="$(FORESIGHT_DB_DSN)" FORESIGHT_CLICKHOUSE_DSN="$(FORESIGHT_CLICKHOUSE_DSN)" exec ./bin/alerter

# Needs the stack running (ingest, processor). Tune with
# FORESIGHT_LOADGEN_TARGETS, _DURATION, _WARMUP, _LOGS_RPS.
load: build
	FORESIGHT_DB_DSN="$(FORESIGHT_DB_DSN)" ./bin/loadgen

test:
	go test -race ./...

# Silent on success, so "make lint" printing nothing means clean.
lint:
	@go vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

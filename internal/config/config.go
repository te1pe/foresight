// Package config reads FORESIGHT_* environment variables.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	DBDSN          string
	ScrapeInterval time.Duration
	HTTPAddr       string
	KafkaBrokers   []string
	KafkaTopic     string
	KafkaLogsTopic string
	KafkaGroup     string
	ClickHouseDSN  string
	QueryHTTPAddr  string
	RedisAddr      string
	// TelegramToken empty means notifications go to the log only.
	TelegramToken  string
	TelegramChatID string
	TelegramAPIURL string
	// AdminAddr overrides the service's default diagnostics address
	// (127.0.0.1:6060 ingest, :6061 processor, :6062 query, :6063 alerter).
	AdminAddr string
}

// The default DSN points at deploy/docker-compose.yml. Port 5433 avoids
// colliding with a PostgreSQL installed natively on the developer's Mac.
const (
	defaultDSN            = "postgres://foresight:foresight@localhost:5433/foresight?sslmode=disable"
	defaultScrapeInterval = 15 * time.Second
	defaultHTTPAddr       = ":8080"
	// The external listener of deploy/docker-compose.yml, reachable from
	// services running natively on the host.
	defaultKafkaBrokers = "localhost:19092"
	defaultKafkaTopic   = "foresight.samples"
	defaultKafkaLogs    = "foresight.logs"
	defaultKafkaGroup   = "foresight-processor"
	defaultClickHouse   = "clickhouse://foresight:foresight@localhost:9000/foresight"
	// The query service runs next to ingest on the same host, so it needs
	// its own port.
	defaultQueryHTTPAddr = ":8081"
	defaultRedisAddr     = "localhost:6379"
	defaultTelegramAPI   = "https://api.telegram.org"
)

// Load takes getenv instead of reading os.Getenv directly so tests can pass
// a map without mutating the process environment.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DBDSN:          withDefault(getenv("FORESIGHT_DB_DSN"), defaultDSN),
		HTTPAddr:       withDefault(getenv("FORESIGHT_HTTP_ADDR"), defaultHTTPAddr),
		ScrapeInterval: defaultScrapeInterval,
		KafkaTopic:     withDefault(getenv("FORESIGHT_KAFKA_TOPIC"), defaultKafkaTopic),
		KafkaLogsTopic: withDefault(getenv("FORESIGHT_KAFKA_LOGS_TOPIC"), defaultKafkaLogs),
		KafkaGroup:     withDefault(getenv("FORESIGHT_KAFKA_GROUP"), defaultKafkaGroup),
		ClickHouseDSN:  withDefault(getenv("FORESIGHT_CLICKHOUSE_DSN"), defaultClickHouse),
		QueryHTTPAddr:  withDefault(getenv("FORESIGHT_QUERY_HTTP_ADDR"), defaultQueryHTTPAddr),
		RedisAddr:      withDefault(getenv("FORESIGHT_REDIS_ADDR"), defaultRedisAddr),
		TelegramToken:  getenv("FORESIGHT_TELEGRAM_TOKEN"),
		TelegramChatID: getenv("FORESIGHT_TELEGRAM_CHAT_ID"),
		TelegramAPIURL: withDefault(getenv("FORESIGHT_TELEGRAM_API_URL"), defaultTelegramAPI),
		AdminAddr:      getenv("FORESIGHT_ADMIN_ADDR"),
		KafkaBrokers:   splitList(withDefault(getenv("FORESIGHT_KAFKA_BROKERS"), defaultKafkaBrokers)),
	}
	// Targets moved to the targets table; a leftover variable would be
	// silently ignored, so refuse it instead.
	if getenv("FORESIGHT_TARGETS") != "" {
		return Config{}, errors.New("FORESIGHT_TARGETS is no longer used: targets live in the targets table in PostgreSQL")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("FORESIGHT_KAFKA_BROKERS: no brokers")
	}
	// A token without a chat would start "successfully" and then fail every
	// send; better to refuse at startup.
	if (cfg.TelegramToken == "") != (cfg.TelegramChatID == "") {
		return Config{}, errors.New("FORESIGHT_TELEGRAM_TOKEN and FORESIGHT_TELEGRAM_CHAT_ID must be set together")
	}

	if raw := getenv("FORESIGHT_SCRAPE_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("FORESIGHT_SCRAPE_INTERVAL: %w", err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("FORESIGHT_SCRAPE_INTERVAL: must be positive, got %s", d)
		}
		cfg.ScrapeInterval = d
	}
	return cfg, nil
}

// splitList parses comma-separated values, ignoring blanks and stray commas.
func splitList(s string) []string {
	var out []string
	for v := range strings.SplitSeq(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func withDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

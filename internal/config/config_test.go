package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr string
	}{
		{
			name: "defaults",
			env:  map[string]string{},
			want: Config{DBDSN: defaultDSN, ScrapeInterval: 15 * time.Second, HTTPAddr: ":8080",
				KafkaBrokers: []string{"localhost:19092"}, KafkaTopic: "foresight.samples", KafkaLogsTopic: "foresight.logs", KafkaGroup: "foresight-processor",
				ClickHouseDSN: defaultClickHouse, QueryHTTPAddr: ":8081",
				RedisAddr: "localhost:6379", TelegramAPIURL: "https://api.telegram.org"},
		},
		{
			name: "overrides",
			env: map[string]string{
				"FORESIGHT_DB_DSN":           "postgres://x@db/y",
				"FORESIGHT_SCRAPE_INTERVAL":  "5s",
				"FORESIGHT_HTTP_ADDR":        "127.0.0.1:9000",
				"FORESIGHT_KAFKA_BROKERS":    "b1:9092,b2:9092",
				"FORESIGHT_KAFKA_TOPIC":      "t",
				"FORESIGHT_KAFKA_LOGS_TOPIC": "l",
				"FORESIGHT_KAFKA_GROUP":      "g",
				"FORESIGHT_CLICKHOUSE_DSN":   "clickhouse://x@ch/y",
				"FORESIGHT_QUERY_HTTP_ADDR":  ":9999",
				"FORESIGHT_REDIS_ADDR":       "r:1",
				"FORESIGHT_TELEGRAM_TOKEN":   "tok",
				"FORESIGHT_TELEGRAM_CHAT_ID": "-1",
				"FORESIGHT_TELEGRAM_API_URL": "http://tg",
			},
			want: Config{DBDSN: "postgres://x@db/y", ScrapeInterval: 5 * time.Second, HTTPAddr: "127.0.0.1:9000",
				KafkaBrokers: []string{"b1:9092", "b2:9092"}, KafkaTopic: "t", KafkaLogsTopic: "l", KafkaGroup: "g",
				ClickHouseDSN: "clickhouse://x@ch/y", QueryHTTPAddr: ":9999",
				RedisAddr: "r:1", TelegramToken: "tok", TelegramChatID: "-1", TelegramAPIURL: "http://tg"},
		},
		{
			name:    "leftover FORESIGHT_TARGETS",
			env:     map[string]string{"FORESIGHT_TARGETS": "localhost:9100"},
			wantErr: "targets table",
		},
		{
			name:    "brokers only separators",
			env:     map[string]string{"FORESIGHT_KAFKA_BROKERS": ","},
			wantErr: "no brokers",
		},
		{
			name:    "telegram token without chat",
			env:     map[string]string{"FORESIGHT_TELEGRAM_TOKEN": "tok"},
			wantErr: "set together",
		},
		{
			name:    "bad interval",
			env:     map[string]string{"FORESIGHT_SCRAPE_INTERVAL": "15"},
			wantErr: "FORESIGHT_SCRAPE_INTERVAL",
		},
		{
			name:    "non-positive interval",
			env:     map[string]string{"FORESIGHT_SCRAPE_INTERVAL": "-1s"},
			wantErr: "must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load(func(k string) string { return tt.env[k] })
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

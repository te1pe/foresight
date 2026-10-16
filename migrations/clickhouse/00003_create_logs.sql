-- +goose NO TRANSACTION

-- +goose Up

-- Logs pushed through POST /v1/logs. Duplicates come from two places: the
-- queue's at-least-once delivery and clients retrying a request whose
-- response they did not get. Both repeat the same line, so the line itself
-- is the identity: ReplacingMergeTree keyed by (service, ts, hash of the
-- raw line) collapses them, and reads that must be exact use FINAL. A line
-- without its own timestamp gets the receive time, so a retried request of
-- such lines cannot be recognised; services should log the time.
CREATE TABLE IF NOT EXISTS logs (
    ts          DateTime64(3, 'UTC'),
    service     LowCardinality(String),
    level       LowCardinality(String),
    message     String,
    attrs       Map(LowCardinality(String), String),
    raw         String,
    received_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(ts)
ORDER BY (service, ts, cityHash64(raw))
TTL toDateTime(ts) + INTERVAL 14 DAY
SETTINGS ttl_only_drop_parts = 1;

-- +goose Down
DROP TABLE IF EXISTS logs;

-- +goose NO TRANSACTION

-- +goose Up

-- One row per rule, series and evaluation minute, firing or not: the
-- history of z-scores and ETAs is what lets a detector be tuned against
-- real data, and the next stage's alerter reads its input from here.
--
-- ReplacingMergeTree on (rule, series, eval_time): evaluation is
-- deterministic for a given minute, so if two processors evaluate the same
-- minute, or one retries it, the rows collapse into one.
CREATE TABLE IF NOT EXISTS detections (
    eval_time   DateTime('UTC'),
    rule        LowCardinality(String),
    kind        LowCardinality(String),  -- forecast | anomaly
    status      LowCardinality(String),  -- exhausts | no_trend | not_enough_data | anomaly | normal | warmup
    firing      Bool,
    target      LowCardinality(String),
    name        LowCardinality(String),
    series_id   UInt64,
    labels      Map(String, String),
    -- forecast: last observed value, fitted value now, slope t-statistic;
    -- anomaly: observed rate, expected (EWMA) rate, z-score.
    observed    Float64,
    expected    Float64,
    score       Float64,
    slope       Float64,                 -- units per second; forecast only
    eta_seconds Nullable(Float64),       -- forecast with status exhausts only
    points      UInt32
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(eval_time)
ORDER BY (target, rule, series_id, eval_time)
TTL eval_time + INTERVAL 30 DAY;

-- +goose Down
DROP TABLE IF EXISTS detections;

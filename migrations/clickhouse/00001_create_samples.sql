-- +goose NO TRANSACTION
-- ClickHouse has no transactional DDL; IF NOT EXISTS makes a half-applied
-- run safe to repeat.

-- +goose Up

-- Raw samples. Deduplication happens on two levels:
--  1. insert_deduplication_token (one token per scrape, see the Go writer)
--     drops a redelivered batch at insert time; the last
--     non_replicated_deduplication_window tokens are remembered;
--  2. ReplacingMergeTree collapses rows with the same sorting key during
--     merges: the backstop for replays older than that window. Reads that
--     must be exact use FINAL.
-- Map cannot be part of a sorting key, so a series is keyed by series_id,
-- a hash of (target, name, sorted labels) computed by the writer.
CREATE TABLE IF NOT EXISTS samples (
    ts        DateTime64(3, 'UTC'),
    target    LowCardinality(String),
    name      LowCardinality(String),
    series_id UInt64,
    labels    Map(LowCardinality(String), String),
    value     Float64,
    type      LowCardinality(String)
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(ts)
ORDER BY (target, name, series_id, ts)
-- Raw points are kept briefly; windows below outlive them.
TTL toDateTime(ts) + INTERVAL 14 DAY
SETTINGS non_replicated_deduplication_window = 10000, ttl_only_drop_parts = 1;

-- Windows are pre-aggregated by materialized views at insert time, so range
-- queries over weeks read a few rows per series instead of every point.
-- min/max/count are simple aggregates; avg and last need a state
-- (sum+count, value+time) that is merged at query time with -Merge.
-- The window tables carry their own deduplication window: with
-- deduplicate_blocks_in_dependent_materialized_views=1 a redelivered scrape
-- is dropped here too. Without it ClickHouse still pushes the duplicate
-- block into the views and count/avg are inflated.
CREATE TABLE IF NOT EXISTS samples_1m (
    target       LowCardinality(String),
    name         LowCardinality(String),
    series_id    UInt64,
    window_start DateTime('UTC'),
    labels       SimpleAggregateFunction(any, Map(String, String)),
    type         SimpleAggregateFunction(any, LowCardinality(String)),
    min          SimpleAggregateFunction(min, Float64),
    max          SimpleAggregateFunction(max, Float64),
    avg          AggregateFunction(avg, Float64),
    last         AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    count        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (target, name, series_id, window_start)
TTL window_start + INTERVAL 90 DAY
SETTINGS non_replicated_deduplication_window = 10000, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS samples_1m_mv TO samples_1m AS
SELECT
    target, name, series_id,
    toStartOfMinute(ts)    AS window_start,
    any(labels)            AS labels,
    any(type)              AS type,
    min(value)             AS min,
    max(value)             AS max,
    avgState(value)        AS avg,
    argMaxState(value, ts) AS last,
    count()                AS count
FROM samples
GROUP BY target, name, series_id, window_start;

CREATE TABLE IF NOT EXISTS samples_5m (
    target       LowCardinality(String),
    name         LowCardinality(String),
    series_id    UInt64,
    window_start DateTime('UTC'),
    labels       SimpleAggregateFunction(any, Map(String, String)),
    type         SimpleAggregateFunction(any, LowCardinality(String)),
    min          SimpleAggregateFunction(min, Float64),
    max          SimpleAggregateFunction(max, Float64),
    avg          AggregateFunction(avg, Float64),
    last         AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    count        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (target, name, series_id, window_start)
TTL window_start + INTERVAL 365 DAY
SETTINGS non_replicated_deduplication_window = 10000, ttl_only_drop_parts = 1;

-- Fed from raw samples, not from samples_1m: a cascade would make the 5m
-- view depend on the 1m view's deduplication behaving identically.
CREATE MATERIALIZED VIEW IF NOT EXISTS samples_5m_mv TO samples_5m AS
SELECT
    target, name, series_id,
    toStartOfFiveMinutes(ts) AS window_start,
    any(labels)              AS labels,
    any(type)                AS type,
    min(value)               AS min,
    max(value)               AS max,
    avgState(value)          AS avg,
    argMaxState(value, ts)   AS last,
    count()                  AS count
FROM samples
GROUP BY target, name, series_id, window_start;

-- +goose Down
DROP VIEW IF EXISTS samples_5m_mv;
DROP TABLE IF EXISTS samples_5m;
DROP VIEW IF EXISTS samples_1m_mv;
DROP TABLE IF EXISTS samples_1m;
DROP TABLE IF EXISTS samples;

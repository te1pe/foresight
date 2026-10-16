-- +goose Up
-- No primary key yet: duplicates are possible only if a batch is retried, and
-- this stage never retries. Idempotent writes arrive together with the queue.
CREATE TABLE samples (
    ts     timestamptz      NOT NULL,
    target text             NOT NULL,
    name   text             NOT NULL,
    labels jsonb            NOT NULL DEFAULT '{}',
    value  double precision NOT NULL,
    type   text             NOT NULL
);

-- Matches the only access pattern so far: one series family of one target
-- over a time range.
CREATE INDEX samples_target_name_ts_idx ON samples (target, name, ts);

-- +goose Down
DROP TABLE samples;

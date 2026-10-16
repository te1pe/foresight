-- +goose Up
-- The queue delivers at least once, so the same scrape can arrive twice.
-- A sample is identified by its series (target, name, labels) and the scrape
-- time; with this key a replayed batch becomes a no-op via ON CONFLICT.
--
-- labels are hashed instead of indexed directly: a btree entry is limited to
-- about 2.7 kB, and one target with long label values would otherwise make
-- every insert of its batch fail. jsonb::text is canonical (keys sorted,
-- duplicates removed), so equal label sets always hash the same.
CREATE UNIQUE INDEX samples_series_ts_key ON samples (target, name, ts, md5(labels::text));

-- (target, name, ts) is a prefix of the new index, so the old one only
-- doubles the write cost.
DROP INDEX samples_target_name_ts_idx;

-- +goose Down
CREATE INDEX samples_target_name_ts_idx ON samples (target, name, ts);
DROP INDEX samples_series_ts_key;

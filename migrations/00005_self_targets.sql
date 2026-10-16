-- +goose Up
-- foresight monitors itself: each service's admin listener (/metrics on
-- 127.0.0.1) becomes a target, so its own rules — goroutine_leak among
-- them — watch its own processes. Delete a row to opt out; a service that
-- is not running simply shows up=0.
INSERT INTO targets (target) VALUES
    ('127.0.0.1:6060'),  -- ingest
    ('127.0.0.1:6061'),  -- processor
    ('127.0.0.1:6062'),  -- query
    ('127.0.0.1:6063')   -- alerter
ON CONFLICT DO NOTHING;

-- +goose Down
DELETE FROM targets WHERE target IN ('127.0.0.1:6060', '127.0.0.1:6061', '127.0.0.1:6062', '127.0.0.1:6063');

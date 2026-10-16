-- +goose Up
-- Scrape targets, replacing the FORESIGHT_TARGETS variable: adding a
-- service (Cloute) becomes an INSERT instead of a redeploy of ingest.
CREATE TABLE targets (
    -- Same format the variable had: "host:port" or a full URL. It is the
    -- value stored as samples.target, so renaming it starts a new history.
    target     text        PRIMARY KEY,
    enabled    boolean     NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Keeps the behaviour of the former default: the local node_exporter.
INSERT INTO targets (target) VALUES ('localhost:9100') ON CONFLICT DO NOTHING;

-- +goose Down
DROP TABLE targets;

-- +goose Up
-- An incident is one continuous period during which a rule fires for one
-- series. The fingerprint (rule, target, series) names that pair.
CREATE TABLE incidents (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fingerprint    text        NOT NULL,
    rule           text        NOT NULL,
    target         text        NOT NULL,
    -- uint64 from ClickHouse does not fit bigint; it is an opaque id anyway.
    series_id      text        NOT NULL,
    labels         jsonb       NOT NULL DEFAULT '{}',
    status         text        NOT NULL CHECK (status IN ('firing', 'resolved')),
    summary        text        NOT NULL,
    opened_at      timestamptz NOT NULL,
    last_eval_at   timestamptz NOT NULL,
    resolved_at    timestamptz,
    resolve_reason text CHECK (resolve_reason IN ('recovered', 'no_data')),
    CHECK ((status = 'resolved') = (resolved_at IS NOT NULL))
);

-- At most one open incident per fingerprint, enforced by the database
-- rather than by application logic: two alerters racing to open the same
-- incident cannot both succeed, and resolved ones may repeat freely.
CREATE UNIQUE INDEX incidents_one_open ON incidents (fingerprint) WHERE status = 'firing';

-- Transactional outbox: an event is written in the same transaction as the
-- state change it describes, so a crash can never leave an incident opened
-- without a pending notification about it. Delivery happens afterwards.
CREATE TABLE incident_events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id bigint      NOT NULL REFERENCES incidents (id),
    kind        text        NOT NULL CHECK (kind IN ('opened', 'resolved')),
    -- Rendered once at transition time, so a retried delivery sends the
    -- exact same text.
    message     text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    notified_at timestamptz
);

-- The delivery loop only ever looks for undelivered events.
CREATE INDEX incident_events_pending ON incident_events (id) WHERE notified_at IS NULL;

-- +goose Down
DROP TABLE incident_events;
DROP TABLE incidents;

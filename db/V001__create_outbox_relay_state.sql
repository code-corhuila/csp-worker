-- Publication state of the booking outbox relay (ADR-014). This is the only schema without a -db
-- repository: it holds no domain data, only what the relay needs to publish at least once and to
-- audit its attempts. The booking schema is never written: the outbox has no processed_at.
-- Run by the instance administrator (tooling profile), never by the worker itself.
CREATE SCHEMA IF NOT EXISTS worker;
ALTER SCHEMA worker OWNER TO worker_app;

CREATE TABLE worker.outbox_relay_state (
    event_id        UUID        NOT NULL,
    source          TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    published_at    TIMESTAMPTZ NULL,
    attempt_count   INTEGER     NOT NULL DEFAULT 0,
    last_error      TEXT        NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_outbox_relay_state PRIMARY KEY (event_id),
    CONSTRAINT ck_outbox_relay_state_status
        CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED_PERMANENT', 'FAILED_EXHAUSTED')),
    CONSTRAINT ck_outbox_relay_state_attempt_count CHECK (attempt_count >= 0)
);

-- The relay asks for the due pending events of one source, oldest attempt first.
CREATE INDEX idx_outbox_relay_state_pending
    ON worker.outbox_relay_state (source, next_attempt_at)
    WHERE status = 'PENDING';

-- The discovery position of the last run, one row per source.
CREATE TABLE worker.outbox_relay_cursor (
    source          TEXT        NOT NULL,
    last_created_at TIMESTAMPTZ NOT NULL,
    last_event_id   UUID        NOT NULL,
    CONSTRAINT pk_outbox_relay_cursor PRIMARY KEY (source)
);

ALTER TABLE worker.outbox_relay_state OWNER TO worker_app;
ALTER TABLE worker.outbox_relay_cursor OWNER TO worker_app;

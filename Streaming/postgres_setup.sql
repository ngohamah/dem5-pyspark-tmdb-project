-- Sets up the database, application role, and events table used by
-- spark_streaming_to_postgres.py.
--
-- Run once, as a Postgres superuser, e.g.:
--   psql -h localhost -p 5432 -U postgres -f postgres_setup.sql
--
-- The role's password is intentionally NOT set here (a real password does
-- not belong in a file tracked by git). After running this script, set it
-- separately and record it in your local .env (see .env.example):
--   ALTER ROLE streaming_app WITH PASSWORD 'your-local-password';

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'streaming_app') THEN
        CREATE ROLE streaming_app WITH LOGIN;
    END IF;
END
$$;

SELECT 'CREATE DATABASE streaming_events OWNER streaming_app'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'streaming_events')
\gexec

\connect streaming_events

CREATE TABLE IF NOT EXISTS events (
    event_id        UUID PRIMARY KEY,
    event_time      TIMESTAMPTZ NOT NULL,
    user_id         INTEGER NOT NULL,
    session_id      UUID NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    product_id      TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    price           NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    total_amount    NUMERIC(10, 2) NOT NULL,
    processing_time TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports the dashboards/queries a stakeholder is most likely to run:
-- "what happened recently" and "how is each category/event type trending".
CREATE INDEX IF NOT EXISTS idx_events_event_time ON events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_category ON events (category);

GRANT CONNECT ON DATABASE streaming_events TO streaming_app;
GRANT USAGE ON SCHEMA public TO streaming_app;
GRANT SELECT, INSERT ON events TO streaming_app;

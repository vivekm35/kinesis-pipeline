-- schema.sql
-- Redshift schema for the events analytics table.
--
-- Design choices:
--   DISTKEY(user_id)   — co-locate rows for the same user on one slice;
--                         most queries filter/join on user_id
--   SORTKEY(event_ts)  — range-restricted scans on time windows (most
--                         reporting queries filter by date/hour)
--   ENCODING           — AZ64 for timestamps/ints; ZSTD for low-cardinality
--                         strings; RAW for high-cardinality IDs

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events (
    event_id        VARCHAR(36)     ENCODE ZSTD      NOT NULL,
    event_type      VARCHAR(64)     ENCODE ZSTD      NOT NULL,
    event_ts        TIMESTAMP       ENCODE AZ64      NOT NULL,
    user_id         VARCHAR(64)     ENCODE ZSTD      NOT NULL  DISTKEY,
    session_id      VARCHAR(36)     ENCODE ZSTD,
    page            VARCHAR(255)    ENCODE ZSTD,
    duration_ms     INTEGER         ENCODE AZ64,
    value           DECIMAL(12, 2)  ENCODE AZ64,
    producer_ver    VARCHAR(16)     ENCODE ZSTD,
    aws_region      VARCHAR(32)     ENCODE ZSTD,
    kinesis_seq     VARCHAR(128)    ENCODE ZSTD,
    processed_by    VARCHAR(64)     ENCODE ZSTD,
    loaded_at       TIMESTAMP       ENCODE AZ64      DEFAULT SYSDATE
)
SORTKEY (event_ts);

-- Separate staging table used during COPY + UPSERT pattern
-- to avoid locking the live table during bulk loads.
CREATE TABLE IF NOT EXISTS analytics.events_staging (LIKE analytics.events);

-- Grant read-only access to reporting role
GRANT SELECT ON analytics.events TO GROUP reporting_users;

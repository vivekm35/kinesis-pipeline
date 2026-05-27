-- redshift/queries/operational_reports.sql
-- -----------------------------------------
-- Ready-to-run reporting queries for the analytics.events table.
-- All queries are designed to complete in <2s with the DISTKEY/SORTKEY
-- tuning applied in schema.sql.

-- ── 1. Events per minute — last 60 minutes ──────────────────────────────────
-- Used in real-time operational dashboards.
SELECT
    DATE_TRUNC('minute', event_ts)          AS minute,
    COUNT(*)                                AS event_count,
    COUNT(DISTINCT user_id)                 AS unique_users,
    SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) AS error_count
FROM analytics.events
WHERE event_ts >= DATEADD(hour, -1, GETDATE())
GROUP BY 1
ORDER BY 1 DESC;


-- ── 2. Top event types — last 24 hours ──────────────────────────────────────
SELECT
    event_type,
    COUNT(*)                                AS total_events,
    COUNT(DISTINCT user_id)                 AS unique_users,
    ROUND(AVG(duration_ms), 1)              AS avg_duration_ms,
    ROUND(AVG(value), 2)                    AS avg_value
FROM analytics.events
WHERE event_ts >= DATEADD(hour, -24, GETDATE())
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;


-- ── 3. Funnel: page_view → add_to_cart → purchase (today) ───────────────────
-- Measures conversion at each step of the purchase funnel.
WITH funnel AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'page_view'    THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'add_to_cart'  THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN event_type = 'purchase'      THEN 1 ELSE 0 END) AS purchased
    FROM analytics.events
    WHERE event_ts >= TRUNC(GETDATE())          -- today only
    GROUP BY user_id
)
SELECT
    SUM(viewed)    AS step1_page_views,
    SUM(carted)    AS step2_add_to_cart,
    SUM(purchased) AS step3_purchase,
    ROUND(100.0 * SUM(carted)    / NULLIF(SUM(viewed),    0), 2) AS view_to_cart_pct,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(carted),    0), 2) AS cart_to_purchase_pct,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(viewed),    0), 2) AS overall_conversion_pct
FROM funnel;


-- ── 4. Revenue by hour — last 7 days ────────────────────────────────────────
SELECT
    DATE_TRUNC('hour', event_ts)            AS hour,
    COUNT(*)                                AS purchase_count,
    ROUND(SUM(value), 2)                    AS total_revenue,
    ROUND(AVG(value), 2)                    AS avg_order_value
FROM analytics.events
WHERE
    event_type = 'purchase'
    AND event_ts >= DATEADD(day, -7, GETDATE())
GROUP BY 1
ORDER BY 1 DESC;


-- ── 5. Pipeline health — duplicate detection ─────────────────────────────────
-- Detects any duplicate event_ids that slipped through idempotency.
-- Should return 0 rows; if not, investigate the Lambda idempotency table.
SELECT
    event_id,
    COUNT(*) AS occurrences,
    MIN(event_ts) AS first_seen,
    MAX(event_ts) AS last_seen,
    MIN(loaded_at) AS first_loaded,
    MAX(loaded_at) AS last_loaded
FROM analytics.events
WHERE loaded_at >= DATEADD(hour, -1, GETDATE())
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY 2 DESC
LIMIT 50;


-- ── 6. Load latency — time from event to Redshift availability ──────────────
-- Measures pipeline end-to-end latency using loaded_at vs event_ts.
SELECT
    DATE_TRUNC('hour', event_ts)            AS event_hour,
    COUNT(*)                                AS events_loaded,
    ROUND(AVG(DATEDIFF(second, event_ts, loaded_at)), 1) AS avg_latency_secs,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DATEDIFF(second, event_ts, loaded_at))::FLOAT, 1) AS p50_latency_secs,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DATEDIFF(second, event_ts, loaded_at))::FLOAT, 1) AS p95_latency_secs,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY DATEDIFF(second, event_ts, loaded_at))::FLOAT, 1) AS p99_latency_secs
FROM analytics.events
WHERE event_ts >= DATEADD(hour, -24, GETDATE())
GROUP BY 1
ORDER BY 1 DESC;


-- ── 7. Active users — rolling 5-minute windows ──────────────────────────────
-- Good for real-time active-user counts (DAU/MAU proxy).
SELECT
    DATE_TRUNC('minute', event_ts)                          AS window_start,
    COUNT(DISTINCT user_id)                                 AS active_users,
    COUNT(*)                                                AS events
FROM analytics.events
WHERE event_ts BETWEEN DATEADD(hour, -1, GETDATE()) AND GETDATE()
GROUP BY 1
ORDER BY 1 DESC
LIMIT 60;


-- ── 8. Error rate — grouped by page, last hour ──────────────────────────────
SELECT
    COALESCE(page, 'unknown')               AS page,
    COUNT(*)                                AS total_events,
    SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) AS errors,
    ROUND(
        100.0 * SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    )                                       AS error_rate_pct
FROM analytics.events
WHERE event_ts >= DATEADD(hour, -1, GETDATE())
GROUP BY 1
HAVING COUNT(*) > 10              -- filter out low-traffic pages
ORDER BY error_rate_pct DESC
LIMIT 20;

-- redshift/queries/maintenance.sql
-- ----------------------------------
-- Run these queries periodically to keep the analytics.events table
-- performant. Schedule via EventBridge → Lambda or Airflow DAG.

-- ── 1. VACUUM — reclaim space and re-sort after bulk loads ──────────────────
-- Run after every large COPY job (> 100k rows) or weekly at minimum.
-- VACUUM SORT ONLY is faster — use when data is mostly sorted already.
VACUUM SORT ONLY analytics.events;

-- Full vacuum (reclaims deleted row space + re-sorts):
-- VACUUM DELETE ONLY analytics.events;
-- VACUUM FULL analytics.events;           -- use only during maintenance window


-- ── 2. ANALYZE — update statistics for the query planner ────────────────────
-- Run after VACUUM or when query plans look suboptimal.
ANALYZE analytics.events;


-- ── 3. Table statistics — check for skew and staleness ──────────────────────
SELECT
    t.schemaname,
    t.tablename,
    s.num_rows,
    s.size_in_mb,
    s.pct_unsorted,
    s.stats_off,
    s.tbl_rows AS actual_rows
FROM SVV_TABLE_INFO t
JOIN (
    SELECT
        tbl,
        SUM(rows) AS num_rows,
        SUM(rows_pre_vacuum) AS tbl_rows,
        ROUND(SUM(size::FLOAT / 1024), 1) AS size_in_mb,
        MAX(unsorted) AS pct_unsorted,
        MAX(stats_off) AS stats_off
    FROM SVV_TABLE_INFO
    GROUP BY tbl
) s ON t.table_id = s.tbl
WHERE t.schemaname = 'analytics'
ORDER BY s.size_in_mb DESC;


-- ── 4. Distribution skew check ───────────────────────────────────────────────
-- High skew means rows aren't evenly distributed; queries will be slow
-- on the overloaded slice. Target: max_rows / avg_rows < 1.5
SELECT
    name                                            AS table_name,
    COUNT(*)                                        AS slice_count,
    MIN(num_rows)                                   AS min_rows_per_slice,
    MAX(num_rows)                                   AS max_rows_per_slice,
    ROUND(AVG(num_rows), 0)                         AS avg_rows_per_slice,
    ROUND(MAX(num_rows)::FLOAT / NULLIF(AVG(num_rows), 0), 2) AS skew_ratio
FROM (
    SELECT
        trim(name) AS name,
        slice,
        num_rows
    FROM stv_tbl_perm
    WHERE name = 'events'
)
GROUP BY name
ORDER BY skew_ratio DESC;


-- ── 5. WLM queue wait times — spot query concurrency issues ─────────────────
SELECT
    service_class,
    num_queued_queries,
    num_executing_queries,
    num_executed_queries,
    total_queue_time / 1000000 AS total_queue_secs,
    total_exec_time / 1000000  AS total_exec_secs
FROM stv_wlm_service_class_state
WHERE service_class > 4
ORDER BY service_class;


-- ── 6. Recent COPY job history ───────────────────────────────────────────────
SELECT
    query,
    substring(filename, 1, 60) AS s3_file,
    lines_scanned,
    lines_loaded,
    lines_failed,
    ROUND(lines_loaded::FLOAT / NULLIF(lines_scanned, 0) * 100, 1) AS success_pct,
    curtime                     AS completed_at
FROM stl_load_commits
WHERE curtime >= DATEADD(hour, -24, GETDATE())
ORDER BY curtime DESC
LIMIT 50;

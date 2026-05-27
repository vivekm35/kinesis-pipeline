"""
copy_job.py
-----------
Orchestrates batched Redshift COPY jobs from the S3 landing zone.

Strategy (COPY → staging → UPSERT):
  1. COPY new S3 files into events_staging (truncate first)
  2. DELETE from events any rows whose event_id already exists in staging
  3. INSERT INTO events SELECT * FROM events_staging
  4. Truncate staging

This pattern avoids duplicate rows on re-runs and keeps the live table
available for reads throughout (no table lock needed).

Can be invoked:
  - By an EventBridge rule on a schedule (e.g. every 60 s)
  - By an S3 event notification via Lambda

Usage:
    python redshift/copy_job.py --s3-prefix s3://bucket/events/year=2024/month=05/day=27/hour=14/
"""

import argparse
import logging
import os
import time

import psycopg2

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# ── Redshift connection params (from env / Secrets Manager) ──────────────────
REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.environ["REDSHIFT_DB"]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]  # injected by Secrets Manager

IAM_ROLE_ARN = os.environ["REDSHIFT_IAM_ROLE"]  # ARN of role with S3 read access
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        connect_timeout=10,
        sslmode="require",
    )


def execute(conn: psycopg2.extensions.connection, sql: str, description: str = "") -> None:
    """Execute a single SQL statement and commit."""
    label = description or sql[:60]
    start = time.monotonic()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        elapsed = time.monotonic() - start
        log.info("✓ %-45s %.2fs", label, elapsed)
    except Exception as exc:
        conn.rollback()
        log.error("✗ %s — %s", label, exc)
        raise


def run_copy_job(s3_prefix: str, dry_run: bool = False) -> dict:
    """
    Execute the full COPY → staging → UPSERT pipeline for one S3 prefix.

    Returns a dict with row counts and timing.
    """
    log.info("Starting COPY job for prefix: %s", s3_prefix)
    start_total = time.monotonic()

    copy_sql = f"""
        COPY analytics.events_staging
        FROM '{s3_prefix}'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS JSON 'auto ignorecase'
        REGION '{AWS_REGION}'
        TIMEFORMAT 'auto'
        TRUNCATECOLUMNS
        COMPUPDATE OFF
        STATUPDATE OFF;
    """

    delete_dupes_sql = """
        DELETE FROM analytics.events
        WHERE event_id IN (
            SELECT event_id FROM analytics.events_staging
        );
    """

    insert_sql = """
        INSERT INTO analytics.events
        SELECT * FROM analytics.events_staging;
    """

    count_sql = "SELECT COUNT(*) FROM analytics.events_staging;"

    if dry_run:
        log.info("[DRY RUN] Would execute COPY from %s", s3_prefix)
        return {"dry_run": True, "s3_prefix": s3_prefix}

    conn = get_connection()
    try:
        execute(conn, "TRUNCATE analytics.events_staging;", "Truncate staging")
        execute(conn, copy_sql, "COPY s3 → staging")

        with conn.cursor() as cur:
            cur.execute(count_sql)
            staged_rows = cur.fetchone()[0]
        conn.commit()
        log.info("Staged %d rows", staged_rows)

        execute(conn, delete_dupes_sql, "Delete duplicates from live table")
        execute(conn, insert_sql, "Insert staging → live table")
        execute(conn, "TRUNCATE analytics.events_staging;", "Clean up staging")

        elapsed = time.monotonic() - start_total
        log.info(
            "COPY job complete — rows=%d total_time=%.2fs",
            staged_rows,
            elapsed,
        )
        return {
            "s3_prefix": s3_prefix,
            "rows_loaded": staged_rows,
            "elapsed_secs": round(elapsed, 3),
        }

    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run a Redshift COPY job")
    parser.add_argument("--s3-prefix", required=True, help="S3 URI prefix to load")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = parser.parse_args()

    result = run_copy_job(args.s3_prefix, dry_run=args.dry_run)
    log.info("Result: %s", result)


if __name__ == "__main__":
    main()

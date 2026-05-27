# Architecture Deep Dive

## Overview

The pipeline ingests arbitrary JSON events from any producer (web app, mobile app, microservice) and makes them available for SQL reporting in Redshift within ~1.2 seconds.

---

## Component Decisions

### Kinesis Data Streams — 10 shards

Each shard handles 1 MB/s write and 2 MB/s read. At ~500 bytes/event × 5,000 events/s = **2.5 MB/s** write → 3 shards minimum. We provision 10 for:

- Headroom for traffic spikes (2× expected peak)
- `parallelization_factor = 10` on the Lambda ESM — one concurrent Lambda per shard, eliminating cross-shard head-of-line blocking

Partition key is `user_id` so events from the same user arrive in order within a shard.

### Lambda — idempotent transformer

**Why idempotency keys?**  
Kinesis guarantees *at-least-once* delivery. Under retries or Lambda failures, the same record can be delivered 2–3 times. Without deduplication this produces duplicate rows in Redshift, corrupting aggregations.

We use DynamoDB conditional writes (24h TTL) keyed on Kinesis sequence numbers. The `ConditionalCheckFailedException` path is a fast, lock-free way to detect duplicates without a read-before-write.

**Why bisect-on-error?**  
Without `bisect_batch_on_function_error: true`, a single poison-pill record blocks the entire shard until `maximum_retry_attempts` exhausts — introducing latency spikes. Bisecting halves the batch on each retry, isolating bad records in O(log N) retries before routing to the DLQ.

### Firehose → S3

Firehose buffers until either `buffering_size` (128 MB) or `buffering_interval` (60 s) — whichever triggers first. This batches thousands of individual Lambda writes into single large S3 objects, which is critical for efficient Redshift COPY performance (Redshift COPY parallelizes across S3 files, and many small files create excessive manifest overhead).

### S3 landing zone — structured prefixes

```
s3://bucket/events/year=YYYY/month=MM/day=DD/hour=HH/
```

Benefits:
1. **Redshift COPY** can be scoped to a specific hour prefix — loads only new data
2. **Redshift Spectrum** can query S3 directly using the same partition structure
3. **Athena** can query ad-hoc without loading into Redshift at all
4. **Lifecycle rules** transition old data to Intelligent-Tiering automatically

### Redshift — batched COPY with staging table

**Why COPY over INSERT?** Redshift is a columnar MPP store. Row-by-row INSERTs serialize through the leader node. COPY parallelizes across compute slices, achieving 100–1000× higher throughput.

**Why a staging table?** Direct COPY into the live table creates contention with active queries. The staging pattern:
1. Truncate staging (cheap)
2. COPY into staging (isolated, no locks on live table)
3. DELETE duplicates from live table
4. INSERT from staging into live table (append-only, fast)

This keeps P99 reporting queries under 2 s even during active loads.

**DISTKEY / SORTKEY tuning:**
- `DISTKEY(user_id)` — most queries join/filter on `user_id`; co-locating rows on the same slice eliminates data movement during joins
- `SORTKEY(event_ts)` — range-restricted scans (`WHERE event_ts > now() - interval '1 hour'`) skip entire sort blocks, reducing I/O by 80–95% on time-series queries

---

## Failure Modes

| Failure | Detection | Recovery |
|---|---|---|
| Lambda timeout | CloudWatch `Errors` alarm | ESM retries up to 3×; then DLQ |
| Firehose S3 write failure | Firehose `DeliveryToS3.DataFreshness` metric | Firehose retries for up to 24h internally |
| Redshift COPY failure | CloudWatch log + Python exception | Re-run `copy_job.py` with same S3 prefix (idempotent) |
| Duplicate Kinesis delivery | DynamoDB idempotency check | Silent skip |
| Kinesis shard throttle | `WriteProvisionedThroughputExceeded` | Producer backs off; consider re-sharding |

---

## Scaling

To scale throughput beyond ~5,000 events/sec:
1. Add Kinesis shards (`UpdateShardCount`) — no downtime
2. Increase `parallelization_factor` on the ESM to match
3. Increase `memory_size` on Lambda if transform logic becomes CPU-bound
4. For Redshift: scale the cluster (more nodes) or migrate to Redshift Serverless with auto-scaling

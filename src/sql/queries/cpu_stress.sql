-- CPU stress query for performance diagnostics.
--
-- Joins tenk1 (1M rows) to tenk2 (10K rows) on `hundred` (100 distinct values),
-- producing ~100M intermediate rows (10K rows/bucket * 100 rows/bucket * 100 buckets).
-- Transcendental functions (sqrt, ln, sin, cos, power) applied per row keep the
-- workload CPU-bound rather than I/O-bound.
--
-- Purpose: generate a query that shows up clearly in pg_stat_statements by
-- total_exec_time, and causes CPU contention when run in two or more parallel sessions.
--
-- Hardware context: tested on a 10-core/16-thread Intel i5-12600KF @ 3.7 GHz, 16 GB RAM.
--
-- work_mem note: set high enough that the hash join stays in memory. If PG spills
-- the hash table to temp files the bottleneck shifts from CPU to disk I/O.
-- A value of 1 GB is sufficient for this query on the above hardware.
--
-- Run with: psql -U postgres -d <dbname> -f src/sql/queries/cpu_stress.sql

SET work_mem = '1GB';

SELECT
    t1.unique1,
    sum(
          sqrt(t1.unique1::float8 + 1.0) * sqrt(t2.unique1::float8 + 1.0)
        + ln((t1.thousand::float8 * t2.thousand::float8) + 2.0)
        + sin(t1.unique1::float8 / 1e5) * cos(t2.unique2::float8 / 1e4)
        + power(t1.fivethous::float8, 0.333) * power(t2.fivethous::float8, 0.333)
    ) AS result
FROM tenk1 t1
JOIN tenk2 t2 ON t1.hundred = t2.hundred
GROUP BY t1.unique1
ORDER BY t1.unique1;

/*
Why this saturates CPU:

Factor	Detail
Hash join fanout	hundred has 100 distinct values → ~10K × 100 = 1M pairs per bucket, 100M rows total
Math per row	sqrt, ln, sin, cos, power — all non-trivial FPU ops, no vectorisation shortcut
No index path	PG will choose a hash join on hundred; no index can help the aggregation
Result set	1M groups × ORDER BY forces a final sort pass on top of the hash aggregate
How it appears in pg_stat_statements:


SELECT query, calls, total_exec_time, mean_exec_time, rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 5;
Look for total_exec_time in the tens-of-thousands of ms range. With two sessions running concurrently you'll also see calls = 2 accumulating. You can also cross-reference with pg_stat_activity while it's running to see state = 'active' and wait_event IS NULL (pure CPU, no waits).

*/
-- Top 10 SQL statements by 9 performance metrics.
-- Filters to DML statements only, excluding system catalog references.
WITH base AS (
    SELECT
        pss.queryid,
        LEFT(pss.query, 200)                                                              AS query_text,
        pss.calls,
        pss.total_exec_time,
        pss.mean_exec_time,
        pss.shared_blks_read                                                              AS disk_reads,
        pss.shared_blks_read::numeric        / NULLIF(pss.calls, 0)                      AS avg_disk_reads,
        pss.shared_blks_hit + pss.shared_blks_read                                       AS buffer_gets,
        (pss.shared_blks_hit + pss.shared_blks_read)::numeric / NULLIF(pss.calls, 0)     AS avg_buffer_gets,
        pss.shared_blks_written                                                           AS disk_writes,
        pss.shared_blks_written::numeric     / NULLIF(pss.calls, 0)                      AS avg_disk_writes,
        GREATEST(pss.total_exec_time - pss.shared_blk_read_time - pss.shared_blk_write_time, 0) AS concurrency_wait_ms
    FROM pg_stat_statements pss
    JOIN pg_database d ON pss.dbid = d.oid
    WHERE d.datname = current_database()
      AND pss.calls > 0
      -- DML only
      AND pss.query ~* '^\s*(SELECT|INSERT|UPDATE|DELETE)\s'
      -- Exclude system catalog / internal references (ILIKE used; \b word-boundary
      -- regex is unreliable with underscore-containing identifiers in PostgreSQL)
      AND pss.query NOT ILIKE '%pg_stat%'
      AND pss.query NOT ILIKE '%pg_catalog%'
      AND pss.query NOT ILIKE '%pg_class%'
      AND pss.query NOT ILIKE '%pg_namespace%'
      AND pss.query NOT ILIKE '%pg_attribute%'
      AND pss.query NOT ILIKE '%pg_type%'
      AND pss.query NOT ILIKE '%pg_proc%'
      AND pss.query NOT ILIKE '%pg_index%'
      AND pss.query NOT ILIKE '%pg_trigger%'
      AND pss.query NOT ILIKE '%pg_constraint%'
      AND pss.query NOT ILIKE '%information_schema%'
      AND pss.query NOT ILIKE '%pgagent%'
      AND pss.query NOT ILIKE '%pg_show_all_settings%'
      AND pss.query NOT ILIKE '%has_table_privilege%'
      AND pss.query NOT ILIKE '%has_schema_privilege%'
      AND pss.query NOT ILIKE '%pg_extension%'
      AND pss.query NOT ILIKE '%pg_settings%'
      AND pss.query NOT ILIKE 'select version()%'
      AND pss.query NOT ILIKE 'select current_schema()%'
)
(SELECT 'elapsed_time'        ::text AS metric, queryid, query_text, calls, total_exec_time       ::numeric AS metric_value FROM base ORDER BY total_exec_time        DESC LIMIT 10)
UNION ALL
(SELECT 'avg_elapsed_time'    ::text,           queryid, query_text, calls, mean_exec_time        ::numeric               FROM base ORDER BY mean_exec_time         DESC LIMIT 10)
UNION ALL
(SELECT 'disk_reads'          ::text,           queryid, query_text, calls, disk_reads            ::numeric               FROM base ORDER BY disk_reads             DESC LIMIT 10)
UNION ALL
(SELECT 'avg_disk_reads'      ::text,           queryid, query_text, calls, avg_disk_reads        ::numeric               FROM base ORDER BY avg_disk_reads         DESC LIMIT 10)
UNION ALL
(SELECT 'buffer_gets'         ::text,           queryid, query_text, calls, buffer_gets           ::numeric               FROM base ORDER BY buffer_gets            DESC LIMIT 10)
UNION ALL
(SELECT 'avg_buffer_gets'     ::text,           queryid, query_text, calls, avg_buffer_gets       ::numeric               FROM base ORDER BY avg_buffer_gets        DESC LIMIT 10)
UNION ALL
(SELECT 'disk_writes'         ::text,           queryid, query_text, calls, disk_writes           ::numeric               FROM base ORDER BY disk_writes            DESC LIMIT 10)
UNION ALL
(SELECT 'avg_disk_writes'     ::text,           queryid, query_text, calls, avg_disk_writes       ::numeric               FROM base ORDER BY avg_disk_writes        DESC LIMIT 10)
UNION ALL
(SELECT 'concurrency_wait_time'::text,          queryid, query_text, calls, concurrency_wait_ms  ::numeric               FROM base ORDER BY concurrency_wait_ms    DESC LIMIT 10)
ORDER BY 1, 5 DESC;

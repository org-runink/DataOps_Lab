-- Optional filters (uncomment and update as needed):
-- set warehouse_filter = 'LOGISTICS_WH'
-- set user_filter      = 'DATAOPS_DBT_USER'

WITH history AS (
  SELECT
    qh.user_name,
    qh.warehouse_name,
    CAST(CONVERT_TIMEZONE('UTC', qh.end_time) AS DATE) AS run_date,
    qh.total_elapsed_time / 1000.0 AS elapsed_seconds,
    CASE WHEN qh.error_code IS NOT NULL THEN 1 ELSE 0 END AS failure_flag
  FROM TABLE(
    INFORMATION_SCHEMA.QUERY_HISTORY(
      DATEADD('second', -604799, CURRENT_TIMESTAMP()),
      CURRENT_TIMESTAMP(),
      10000
    )
  ) AS qh
  WHERE qh.database_name = '__database__'
    -- AND qh.warehouse_name = $warehouse_filter
    -- AND qh.user_name      = $user_filter
),

q AS (
  SELECT
    run_date,
    user_name,
    warehouse_name,
    COUNT(*) AS total_queries,
    AVG(elapsed_seconds) AS avg_query_duration_sec,
    SUM(failure_flag) AS failed_queries
  FROM history
  GROUP BY run_date, user_name, warehouse_name
),

storage AS (
  SELECT
    CURRENT_DATE AS as_of_date,
    SUM(active_bytes + time_travel_bytes + failsafe_bytes) AS total_bytes
  FROM information_schema.table_storage_metrics
  WHERE table_catalog = '__database__'
    AND table_schema ILIKE '__schema_prefix__%'
),

q_agg AS (
  SELECT
    run_date,
    SUM(total_queries) AS total_queries,
    AVG(avg_query_duration_sec) AS avg_query_duration_sec,
    SUM(failed_queries) AS failed_queries
  FROM q
  GROUP BY run_date
),

week_kpis AS (
  SELECT
    MAX(run_date) AS last_date,
    SUM(total_queries) AS wk_total_queries,
    AVG(avg_query_duration_sec) AS wk_avg_query_duration_sec,
    SUM(failed_queries) AS wk_failed_queries
  FROM q_agg
)

SELECT
  wk.last_date,
  wk.wk_total_queries,
  wk.wk_failed_queries,
  ROUND(wk.wk_avg_query_duration_sec, 3) AS wk_avg_query_duration_sec,
  ROUND(st.total_bytes / 1024.0 / 1024.0 / 1024.0, 3) AS storage_gb_estimate
FROM week_kpis AS wk
CROSS JOIN storage AS st;

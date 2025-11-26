{{ config(materialized='table') }}

with d as (
  select
    report_date,
    sum(total_orders)          as total_orders,
    sum(total_units_shipped)   as units_shipped,
    avg(stock_turnover_ratio)  as stock_turnover_ratio
  from {{ ref('obs_kpis_daily') }}
  where report_date >= dateadd('day', -7, current_date())
  group by 1
)

select
  max(report_date)            as last_date,
  sum(total_orders)           as wk_orders,
  sum(units_shipped)          as wk_units,
  avg(stock_turnover_ratio)   as avg_turnover
from d

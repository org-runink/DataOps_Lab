{{ config(materialized='table') }}

-- Daily KPI fact: one row per (report_date, warehouse, product)
with k as (
  select
    report_date,
    warehouse_name,
    product_name,
    category,
    total_orders,
    total_units_shipped,
    total_units_replenished,
    case
      when nullif(total_units_replenished, 0) is null then 0
      else round(total_units_shipped / nullif(total_units_replenished, 0), 2)
    end as stock_turnover_ratio
  from {{ ref('daily_inventory_kpis') }}   -- <- comes from GOLD
)

select * from k

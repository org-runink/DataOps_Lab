{{ config(materialized='table') }}

select
  current_date()                            as as_of_date,
  max(movement_date)                        as latest_movement_date,
  datediff('hour', max(movement_date), current_timestamp()) as freshness_hours
from {{ ref('daily_inventory_snapshot') }}   -- <- from SILVER

-- Stage FROM Bronze Snowflake table (not seeds)
with raw as (
  select
    movement_id,
    product_id,
    warehouse_id,
    movement_date,
    quantity,
    movement_type
  from {{ ref('inventory_movements') }}
)

select
  movement_id,
  product_id,
  warehouse_id,
  cast(movement_date as date) as movement_date,
  cast(quantity as int)       as quantity,
  movement_type
from raw

-- Stage FROM Bronze Snowflake table (not seeds)
with raw as (
  select
    warehouse_id,
    warehouse_name,
    location,
    capacity_units,
    manager_name
  from {{ ref('warehouses') }}
)

select
  warehouse_id,
  warehouse_name,
  location,
  capacity_units,
  manager_name
from raw

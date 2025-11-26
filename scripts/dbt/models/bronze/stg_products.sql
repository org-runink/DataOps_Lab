-- Stage FROM Bronze Snowflake table (not seeds)
with raw as (
  select
    product_id,
    product_name,
    category,
    unit_cost,
    weight_kg,
    dimensions_cm
  from {{ ref('products') }}
)

select
  product_id,
  product_name,
  category,
  unit_cost,
  weight_kg,
  dimensions_cm
from raw

-- Stage FROM Bronze Snowflake table (not seeds)
with raw as (
  select
    order_id,
    product_id,
    warehouse_id,
    order_date,
    quantity,
    sales_channel
  from {{ ref('customer_orders') }}
)

select
  order_id,
  product_id,
  warehouse_id,
  cast(order_date as date) as order_date,
  cast(quantity as int)    as quantity,
  sales_channel
from raw

-- scripts/dbt/models/silver/daily_inventory_snapshot.sql

WITH inventory AS (
    SELECT
        movement_date,
        warehouse_id,
        product_id,
        SUM(CASE WHEN movement_type = 'replenishment' THEN quantity ELSE 0 END) AS qty_replenished,
        SUM(CASE WHEN movement_type = 'outbound'      THEN ABS(quantity) ELSE 0 END) AS qty_shipped,
        SUM(CASE WHEN movement_type = 'adjustment'    THEN quantity ELSE 0 END) AS qty_adjusted
    FROM {{ ref('stg_inventory_movements') }}
    GROUP BY movement_date, warehouse_id, product_id
),

orders AS (
    SELECT
        order_date   AS movement_date,
        warehouse_id,
        product_id,
        SUM(quantity) AS qty_ordered
    FROM {{ ref('stg_customer_orders') }}
    GROUP BY order_date, warehouse_id, product_id
)

SELECT
    inv.movement_date,
    inv.warehouse_id,
    wh.warehouse_name,
    inv.product_id,
    prd.product_name,
    prd.category                                                  AS product_category,
    COALESCE(inv.qty_replenished, 0)                              AS qty_replenished,
    COALESCE(inv.qty_shipped, 0)                                  AS qty_shipped,
    COALESCE(inv.qty_adjusted, 0)                                 AS qty_adjusted,
    COALESCE(ord.qty_ordered, 0)                                  AS qty_ordered,
    COALESCE(inv.qty_replenished, 0)
      - COALESCE(inv.qty_shipped, 0)
      + COALESCE(inv.qty_adjusted, 0)                             AS net_inventory_change
FROM inventory inv
LEFT JOIN orders ord
  ON  inv.movement_date = ord.movement_date
  AND inv.warehouse_id  = ord.warehouse_id
  AND inv.product_id    = ord.product_id
LEFT JOIN {{ ref('stg_products') }} prd
  ON inv.product_id = prd.product_id
LEFT JOIN {{ ref('stg_warehouses') }} wh
  ON inv.warehouse_id = wh.warehouse_id

-- gold‑layer KPIs summarised from the daily snapshot

SELECT
    movement_date AS report_date,
    warehouse_name,
    product_name,
    product_category AS category,
    SUM(qty_ordered)     AS total_orders,
    SUM(qty_shipped)     AS total_units_shipped,
    SUM(qty_replenished) AS total_units_replenished,
    AVG(net_inventory_change) AS avg_daily_inventory_change,
    CASE
        WHEN SUM(qty_replenished) > 0
             THEN ROUND(SUM(qty_shipped) / NULLIF(SUM(qty_replenished), 0), 2)
        ELSE 0
    END AS stock_turnover_ratio
FROM {{ ref('daily_inventory_snapshot') }}
GROUP BY report_date, warehouse_name, product_name, product_category
ORDER BY report_date DESC, warehouse_name, product_name

create_dm_order_within2years_table = """
CREATE TABLE IF NOT EXISTS dm_order_within2year (
    order_nums int,
    category VARCHAR,
    date timestamp,
UNIQUE(date, category)
);
"""

transform_dm_order_within2years_table = """
INSERT INTO dm_order_within2year(order_nums, category, date)
SELECT sum(1) AS order_nums,
    category,
    '{{ yesterday_ds }}' AS date
FROM fact_orders_created inner join dim_products on fact_orders_created.product_id = dim_products.id
WHERE fact_orders_created.created_time >= DATE '{{ yesterday_ds }}' - interval '2 years'
GROUP BY category
ON CONFLICT(date, category) 
DO UPDATE SET order_nums = excluded.order_nums;

INSERT INTO dm_order_within2year(order_nums, category, date)
SELECT sum(1) AS order_nums,
    NULL AS category,
    '{{ yesterday_ds }}' AS date
FROM fact_orders_created inner join dim_products on fact_orders_created.product_id = dim_products.id
WHERE fact_orders_created.created_time >= DATE '{{ yesterday_ds }}' - interval '2 years'
ON CONFLICT(date, category)  -- 有坑, null 不等于 null, 这里会插入重复数据
DO UPDATE SET order_nums = excluded.order_nums;
"""

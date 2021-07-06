create_dm_uncompleted_order_created_before_this_month_table = """
CREATE TABLE IF NOT EXISTS dm_uncompleted_order_created_before_this_month (
    uncompleted_order_nums int,
    date timestamp,
UNIQUE(date)
);
"""

transform_dm_uncompleted_order_created_before_this_month_able = """
INSERT INTO dm_uncompleted_order_created_before_this_month(uncompleted_order_nums, date)
SELECT sum(1) AS uncompleted_order_nums,
    '{{ ds }}' AS date
FROM dim_orders INNER JOIN fact_orders_created on dim_orders.order_id = fact_orders_created.order_id
WHERE fact_orders_created.created_time <= '{{ yesterday_ds }}'
    AND dim_orders.start_time <= '{{ yesterday_ds }}' AND dim_orders.end_time > '{{ yesterday_ds }}'
    AND status != 'completed'
ON CONFLICT(date) 
DO UPDATE SET uncompleted_order_nums = excluded.uncompleted_order_nums;
"""

create_dm_uncompleted_order_created_within_this_month_table = """
CREATE TABLE IF NOT EXISTS dm_uncompleted_order_created_within_this_month (
    uncompleted_order_nums int,
    date timestamp,
UNIQUE(date)
);
"""

transform_dm_uncompleted_order_created_within_this_month_able = """
INSERT INTO dm_uncompleted_order_created_within_this_month(uncompleted_order_nums, date)
SELECT sum(1) AS uncompleted_order_nums,
    '{{ ds }}' AS date
FROM dim_orders INNER JOIN fact_orders_created on dim_orders.order_id = fact_orders_created.order_id
WHERE fact_orders_created.created_time > '{{ yesterday_ds }}'
    AND dim_orders.start_time <= '{{ yesterday_ds }}' AND dim_orders.end_time > '{{ yesterday_ds }}'
    AND status != 'completed'
ON CONFLICT(date) 
DO UPDATE SET uncompleted_order_nums = excluded.uncompleted_order_nums;
"""
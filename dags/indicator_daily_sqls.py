create_dm_uncompleted_order_table = """
CREATE TABLE IF NOT EXISTS dm_uncompleted_order (
    uncompleted_order_nums int,
    date timestamp,
UNIQUE(date)
);
"""

transform_dm_uncompleted_order_table = """
INSERT INTO dm_uncompleted_order(uncompleted_order_nums, date)
SELECT sum(1) AS uncompleted_order_nums,
    '{{ yesterday_ds }}' AS date
FROM dim_orders
WHERE dim_orders.start_time <= '{{ yesterday_ds }}' AND dim_orders.end_time > '{{ yesterday_ds }}'
    AND status != 'completed'
ON CONFLICT(date) 
DO UPDATE SET uncompleted_order_nums = excluded.uncompleted_order_nums;
"""

create_dm_inventory_table = """
CREATE TABLE IF NOT EXISTS dm_inventory (
    category VARCHAR,
    amount int,
    date timestamp,
UNIQUE(category, date)
);
"""

transform_dm_inventory_table = """
INSERT INTO dm_inventory(category, amount, date)
SELECT category, 
    sum(1) as amount,  
    '{{ yesterday_ds }}' AS date
FROM (SELECT productid, amount, date, rank() over(PARTITION BY productid ORDER BY date DESC) as rank
      FROM dim_inventory
      WHERE date <= '{{ yesterday_ds }}') temp 
    INNER JOIN dim_products 
        ON temp.productid = dim_products.id
WHERE rank = 1 
GROUP BY category  
ON CONFLICT(category, date) 
DO UPDATE SET amount = excluded.amount;
"""

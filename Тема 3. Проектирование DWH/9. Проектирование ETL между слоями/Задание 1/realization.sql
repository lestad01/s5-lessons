INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, 
orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
SELECT 
    o.restaurant_id                         AS restaurant_id,
    r.restaurant_name                       AS restaurant_name,
    t.date                                  AS settlement_date,
    COUNT(DISTINCT o.id)                    AS orders_count,
    SUM(ps.total_sum)                       AS orders_total_sum,
    SUM(ps.bonus_payment)                   AS orders_bonus_payment_sum,
    SUM(ps.bonus_grant)                     AS orders_bonus_granted_sum,
    SUM(ps.total_sum) * 0.25                AS order_processing_fee,
    (SUM(ps.total_sum) - SUM(ps.total_sum) 
    * 0.25 - SUM(ps.bonus_payment))         AS restaurant_reward_sum
FROM 
    dds.fct_product_sales ps
JOIN
    dds.dm_orders o ON o.id = ps.order_id
JOIN
    dds.dm_restaurants r ON o.restaurant_id = r.id
JOIN
    dds.dm_timestamps t ON o.timestamp_id = t.id

WHERE 
    o.order_status = 'CLOSED'
GROUP BY 
    o.restaurant_id, r.restaurant_name, t.date
ON CONFLICT (restaurant_id, settlement_date) 
DO UPDATE SET 
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
 WITH test_results AS (
    SELECT 
        a.id AS actual_id,
        e.id AS expected_id,
        COALESCE(a.restaurant_id, e.restaurant_id) AS restaurant_id,
        COALESCE(a.restaurant_name, e.restaurant_name) AS restaurant_name,
        COALESCE(a.settlement_year, e.settlement_year) AS settlement_year,
        COALESCE(a.settlement_month, e.settlement_month) AS settlement_month,
        COALESCE(a.orders_count, e.orders_count) AS orders_count,
        COALESCE(a.orders_total_sum, e.orders_total_sum) AS orders_total_sum,
        COALESCE(a.orders_bonus_payment_sum, e.orders_bonus_payment_sum) AS orders_bonus_payment_sum,
        COALESCE(a.orders_bonus_granted_sum, e.orders_bonus_granted_sum) AS orders_bonus_granted_sum,
        COALESCE(a.order_processing_fee, e.order_processing_fee) AS order_processing_fee,
        COALESCE(a.restaurant_reward_sum, e.restaurant_reward_sum) AS restaurant_reward_sum
    FROM 
        public_test.dm_settlement_report_actual a
    FULL OUTER JOIN 
        public_test.dm_settlement_report_expected e
        ON a.restaurant_id = e.restaurant_id
        AND a.settlement_year = e.settlement_year
        AND a.settlement_month = e.settlement_month
        AND a.orders_count = e.orders_count
        AND a.orders_total_sum = e.orders_total_sum
        AND a.orders_bonus_payment_sum = e.orders_bonus_payment_sum
        AND a.orders_bonus_granted_sum = e.orders_bonus_granted_sum
        AND a.order_processing_fee = e.order_processing_fee
        AND a.restaurant_reward_sum = e.restaurant_reward_sum
)
SELECT 
    current_timestamp AS test_date_time,
    'test_01' AS test_name,
    CASE 
        WHEN COUNT(*) = 0 THEN TRUE
        ELSE FALSE
    END AS test_result
FROM 
    test_results
WHERE 
    actual_id IS NULL
    OR expected_id IS NULL;
SELECT
    subscriptions.customer_id,
    subscriptions.subscription_id, 
    -- date imputation based on order_date; start_date and order_date equivalence confirmed in exploratory analyses
    DATE_TRUNC(COALESCE(subscriptions.start_date, orders.order_date), MONTH) AS start_date,
    DATE_TRUNC(
        COALESCE(
            subscriptions.end_date,
            DATE_ADD(orders.order_date, INTERVAL 12 MONTH)),
        MONTH) AS end_date,
    orders.mrr
FROM {{ ref('int_orders_enriched') }} AS orders
INNER JOIN {{ ref('stg_subscriptions') }} AS subscriptions
    ON orders.subscription_id = subscriptions.subscription_id
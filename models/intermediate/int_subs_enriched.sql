SELECT
    subscriptions.customer_id,
    orders.subscription_id, -- from orders because some subscription_ids only present in this table
    subscriptions.plan_name,
    subscriptions.number_of_licenses,
    DATE_TRUNC(COALESCE(subscriptions.start_date, orders.order_date), MONTH) AS start_date,
    DATE_TRUNC(
        COALESCE(
            subscriptions.end_date,
            DATE_ADD(orders.order_date, INTERVAL 12 MONTH)),
        MONTH) AS end_date,
    orders.mrr
FROM {{ ref('int_orders_enriched') }} AS orders
LEFT JOIN {{ ref('stg_subscriptions') }} AS subscriptions
    ON orders.subscription_id = subscriptions.subscription_id
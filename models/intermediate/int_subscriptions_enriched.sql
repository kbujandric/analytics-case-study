SELECT
    subscriptions.customer_id,
    subscriptions.subscription_id,
    subscriptions.plan_name,
    subscriptions.number_of_licenses,
    COALESCE(subscriptions.start_date,
        orders.order_date
    ) AS start_date,
    COALESCE(
        subscriptions.end_date,
        DATE_ADD(orders.order_date, INTERVAL 12 MONTH)
    ) AS end_date,
    orders.mrr
FROM {{ ref('int_orders_enriched') }} AS orders
LEFT JOIN {{ ref('stg_subscriptions') }} AS subscriptions
    ON orders.subscription_id = subscriptions.subscription_id
ORDER BY start_date
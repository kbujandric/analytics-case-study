SELECT
    subscription_id,
    order_id,
    order_date,
    gross_amount,
    (gross_amount - gross_amount * tax_percentage) * exchange_rate AS net_revenue,
    (gross_amount - gross_amount * tax_percentage) * exchange_rate / 12 AS mrr
FROM {{ ref('stg_orders') }}
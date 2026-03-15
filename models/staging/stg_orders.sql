WITH deduped AS (
    SELECT 
        DISTINCT *
    FROM {{ source("ninox", "orders") }}
),

parsed AS (
    SELECT
        subscription_id,
        order_id,
        DATE(order_date) AS order_date,
        gross_amount,
        CAST(JSON_VALUE(checkout_metadata, '$.exchange_rate') AS FLOAT64) AS exchange_rate,
        CAST(JSON_VALUE(checkout_metadata, '$.tax_percentage') AS FLOAT64) AS tax_percentage
    FROM deduped
)

SELECT
    *
FROM parsed
WHERE tax_percentage IS NOT NULL
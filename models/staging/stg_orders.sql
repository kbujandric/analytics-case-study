WITH deduped AS (
  SELECT 
    DISTINCT *
  FROM {{ source("ninox", "orders") }}
)

SELECT
    subscription_id,
    order_id,
    order_date,
    gross_amount,
    CAST(JSON_VALUE(checkout_metadata, '$.exchange_rate') AS FLOAT64) AS exchange_rate,
    CAST(JSON_VALUE(checkout_metadata, '$.tax_percentage') AS FLOAT64) AS tax_percentage
FROM deduped
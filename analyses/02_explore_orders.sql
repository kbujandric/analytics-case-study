------------------------------------------
-- DUPLICATION
------------------------------------------
SELECT 
    *
FROM (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY subscription_id) AS num_rows
    FROM {{ source("ninox", "orders") }}
)
WHERE num_rows > 1
ORDER BY num_rows DESC
-- duplication is present

-- how many order_ids are not unique?
SELECT
  order_id,
  COUNT(*) AS num_rows
FROM {{ source("ninox", "orders") }}
GROUP BY order_id
HAVING num_rows > 1;
-- 11 order_ids with multiple rows in orders


-- are these rows *complete* duplicates?
-- i.e., are all columns in duplicated orders the same

-- if all count(distinct column) columns = 1, that means that the duplicate rows are fully identical
-- advantage: able to see exactly in which columns the "duplicate" rows are potentially different
SELECT
    order_id,
    COUNT(*) AS total_rows, -- number of rows with same order_id (grouped by)
    COUNT(DISTINCT subscription_id) AS distinct_subscription_id,
    COUNT(DISTINCT order_date) AS distinct_order_date,
    COUNT(DISTINCT gross_amount) AS distinct_gross_amount,
    COUNT(DISTINCT checkout_metadata) AS distinct_checkout_metadata,
FROM {{ source("ninox", "orders") }}
GROUP BY order_id
HAVING total_rows > 1 -- find those order_ids that appear more than once
-- all DISTINCT columns = 1, meaning deduplication is safe and necessary
-- could be verified by CTE, counting those where any of DISTINCT columns > 1

 ------------------------------------------
-- MISSINGNESS 
------------------------------------------
SELECT
    COUNTIF(order_id IS NULL) AS order_id, 
    COUNTIF(subscription_id IS NULL) AS subscription_id, 
    COUNTIF(order_date IS NULL) AS order_date, 
    COUNTIF(gross_amount IS NULL) AS gross_amount,
    COUNTIF(checkout_metadata IS NULL) AS checkout_metadata,
    COUNTIF(JSON_VALUE(checkout_metadata, '$.exchange_rate') IS NULL) AS exchange_rate,
    COUNTIF(JSON_VALUE(checkout_metadata, '$.tax_percentage') IS NULL) AS tax_percentage,
    COUNTIF(JSON_VALUE(checkout_metadata, '$.currency') IS NULL) AS currency
FROM {{ source("ninox", "orders") }}
-- only missing tax_percentage for 11 rows
-- how big of a problem is this?

WITH parsed AS (
    SELECT
        subscription_id,
        order_id,
        DATE(order_date) AS order_date,
        gross_amount,
        CAST(JSON_VALUE(checkout_metadata, '$.exchange_rate') AS FLOAT64) AS exchange_rate,
        CAST(JSON_VALUE(checkout_metadata, '$.tax_percentage') AS FLOAT64) AS tax_percentage
    FROM (
        SELECT 
            DISTINCT *
        FROM {{ source("ninox", "orders") }})
        )
    )
SELECT
    tax_percentage,
    COUNT(*) AS frequency
FROM parsed
GROUP BY tax_percentage
-- out of >700 orders, 614 have tax_percentage = 0, and only 11 are NULL
-- I would flag this as an upstream problem and discard these rows, 
-- at least for this case study's purposes
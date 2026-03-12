WITH last_subscriptions AS (
    SELECT
        *,
        MAX(end_date) OVER (PARTITION BY customer_id) AS max_end_date -- find last month of last subscription within customer
    FROM {{ ref('int_subs_enriched') }}
)

SELECT
    customer_id,
    subscription_id,
    start_date,
    end_date,
    mrr,
    DATE_TRUNC(end_date, MONTH) AS month
FROM {{ ref('int_subs_monthly') }}

UNION ALL -- extra month with 0 MRR at end of customer's last subscription to be able to track lost MRR (churn)

SELECT 
    customer_id, 
    subscription_id, 
    start_date,
    end_date,
    0 AS mrr, 
    DATE_TRUNC(end_date, MONTH) AS month
FROM last_subscriptions
WHERE end_date = max_end_date
ORDER BY customer_id, subscription_id, start_date, month
WITH expanded AS (
    -- use array of numbers 0 to 11 to create 12 duplicate rows per each subscription,
    -- each row corresponding to a calendar month of the subscription's duration
    SELECT 
        customer_id,
        subscription_id,
        start_date,
        end_date,
        mrr,
        DATE_ADD(start_date, INTERVAL month_offset MONTH) AS month 
    FROM {{ ref('int_subscriptions_enriched') }}
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, 11)) AS month_offset
),

last_subscriptions AS (
    SELECT
        *,
        MAX(end_date) OVER (PARTITION BY customer_id) AS max_end_date -- find last month of last subscription within customer
    FROM {{ ref('int_subscriptions_enriched') }}
)

SELECT
    customer_id,
    subscription_id,
    start_date,
    end_date,
    mrr,
    month
FROM expanded

UNION ALL -- extra month with 0 MRR at end of customer's last subscription to be able to track lost MRR

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
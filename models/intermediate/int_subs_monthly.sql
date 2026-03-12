-- use array of numbers 0 to 11 to create 12 duplicate rows per each subscription,
-- each row corresponding to a calendar month of the subscription's duration
SELECT 
    customer_id,
    subscription_id,
    start_date,
    end_date,
    mrr,
    DATE_ADD(start_date, INTERVAL month_offset MONTH) AS month 
FROM {{ ref('int_subs_enriched') }}
CROSS JOIN UNNEST(GENERATE_ARRAY(0, 11)) AS month_offset
ORDER BY customer_id, subscription_id, start_date, month
WITH first_sub AS (
    SELECT 
        customer_id,
        DATE_TRUNC(MIN(start_date), MONTH) AS cohort
        FROM {{ ref ("int_subs_monthly") }}
    GROUP BY customer_id
)

SELECT 
    monthly.customer_id,
    monthly.mrr,
    first_sub.cohort,
    DATE_DIFF(monthly.month, first_sub.cohort, MONTH) AS lifetime_month
FROM {{ ref ("int_subs_monthly") }} AS monthly
LEFT JOIN first_sub 
    ON monthly.customer_id = first_sub.customer_id
WHERE monthly.customer_id IS NOT NULL
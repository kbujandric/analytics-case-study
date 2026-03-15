WITH grouped AS(
    SELECT 
        cohort,
        lifetime_month,
        COUNT(DISTINCT customer_id) AS cohort_size,
        SUM(mrr) AS total_mrr
    FROM {{ ref ("int_customer_cohort_monthly") }}
    GROUP BY cohort, lifetime_month
)

SELECT 
    *,
    total_mrr / FIRST_VALUE(total_mrr) OVER (PARTITION BY cohort ORDER BY lifetime_month) * 100 AS pct_retention
FROM grouped
ORDER BY cohort, lifetime_month
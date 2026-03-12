WITH partition_customer AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date, month) AS row_num, -- find overall first/last month for customer
        LAG(subscription_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS prev_subs_id, -- determine (dis)continuity of subscription
        LAG(mrr) OVER (PARTITION BY customer_id ORDER BY start_date, month) AS prev_mrr -- previous month's MRR, used to calculate MRR change
    FROM {{ ref ('int_subs_monthly_extended') }}
)

SELECT
    customer_id,
    subscription_id,
    month,
    mrr, 
    prev_mrr, 
    mrr AS end_mrr,

    CASE -- Start of period MRR is always previous month's MRR, EXCEPT for first month of customer's first subscription
        WHEN row_num = 1 THEN 0
        ELSE prev_mrr 
    END AS start_mrr,


    CASE -- change in MRR between months 
        WHEN row_num = 1 THEN mrr -- first month of customer's first subscription: whole of subscription's MRR gained
        ELSE mrr - prev_mrr -- otherwise compare this month to previous
    END AS mrr_change,  
    
    -- categorizing MRR movements into 4 movement types
    CASE 
        WHEN row_num = 1 THEN "new" -- first month of customer's first subscription
        WHEN prev_subs_id != subscription_id THEN -- beginning of new subscription: compare MRRs
            CASE 
                WHEN mrr = prev_mrr THEN "unchanged"
                WHEN mrr < prev_mrr THEN "contraction"
                WHEN mrr > prev_mrr THEN "expansion"
                ELSE NULL
            END
        WHEN MAX(row_num) OVER (PARTITION BY customer_id) = row_num THEN "lost" -- last row per customer = zero-MRR churn row added in int_subs_monthly_extended
        ELSE "mid_subscription" -- just for sanity checks
    END AS change_type
    
FROM partition_customer
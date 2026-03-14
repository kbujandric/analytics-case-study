------------------------------------------
-- DUPLICATION
------------------------------------------
SELECT
  *
FROM (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY subscription_id) AS count
    FROM {{ source("ninox", "subscriptions") }} AS subs
) AS subs
WHERE subs.count > 1
ORDER BY subs.count DESC 
-- nothing returned, so all unique

SELECT 
    COUNT(DISTINCT subscription_id) AS num_subs 
FROM {{ source("ninox", "subscriptions") }} 
-- 709 unique subscriptions


------------------------------------------
-- MISSINGNESS 
------------------------------------------
SELECT
    COUNTIF(subscription_id IS NULL) AS subscription_id,
    COUNTIF(customer_id IS NULL) AS customer_id,
    COUNTIF(plan_name IS NULL) AS plan_name,
    COUNTIF(number_of_licenses IS NULL) AS number_of_licenses,
    COUNTIF(start_date IS NULL) AS start_date,
    COUNTIF(end_date IS NULL) AS end_date,
FROM {{ source("ninox", "subscriptions") }}
-- only 9 nulls in start_date and end_date, 
-- let's check whether it's always the same subscriptions where both are missing

SELECT 
    subscription_id,
    start_date,
    end_date
FROM {{ source("ninox", "subscriptions") }} 
WHERE start_date IS NULL
-- yes, the same 9 with missing start_date also have missing end_date


------------------------------------------
-- BUSINESS RULE VALIDATION/EXPLORATION 
------------------------------------------
SELECT
    MIN(DATE_DIFF(end_date, start_date, DAY)) AS min_dur,
    MAX(DATE_DIFF(end_date, start_date, DAY)) AS max_dur
FROM {{ source("ninox", "subscriptions") }} 
-- all of them are exactly 12 months (365 days) long

-- OTHER TESTS REGARDING SUBSCRIPTION DURATION / GAP:
-- explored later, after joining orders and subscriptions (date imputation):
    -- can a customer have >1 active subscription at a time?
    -- a gap between end of one and start of next subscription? "ressurections" 
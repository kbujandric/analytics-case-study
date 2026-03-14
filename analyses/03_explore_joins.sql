------------------------------------------
-- MISMATCHING ROWS
------------------------------------------
-- combining the tables using full outer join,
-- then finding those rows that have no matches ("orphans")
-- and marking them to clarify type of mismatch
SELECT
    orders.order_id,
    subscriptions.subscription_id,
    CASE
        WHEN subscriptions.subscription_id IS NULL THEN 'order has no subscription'
        WHEN orders.order_id IS NULL THEN 'subscription has no order'
    END AS mismatch_type
FROM {{ source("ninox", "orders") }} AS orders
FULL OUTER JOIN {{ source("ninox", "subscriptions") }} AS subscriptions 
    USING (subscription_id)
WHERE 
    orders.order_id IS NULL 
    OR 
    subscriptions.subscription_id IS NULL
-- 2 orphaned orders: have information on transaction, but no information on customer_id
-- DECISION: drop these rows for these analyses because customer identity is crucial;
-- MRR movements depend on having continuity *within customer*
-- MRR cohort retention depend on identifying *customer's* first subscription


------------------------------------------
-- DATE IMPUTATION
------------------------------------------
-- some subscriptions are missing start_date and end_date
-- worth checking whether start_date = order_date for non-missing data
WITH everything AS (
    SELECT
        orders.*, 
        subscriptions.start_date,
        subscriptions.end_date
    FROM {{ source("ninox", "orders") }} AS orders
    FULL OUTER JOIN {{ source("ninox", "subscriptions") }} AS subscriptions
    ON orders.subscription_id = subscriptions.subscription_id
)
SELECT *
FROM everything
WHERE 
    start_date != DATE(order_date) -- because order_date is datetime
    OR start_date IS NULL
    OR order_date IS NULL; 
-- the order date and subscription start date are always equal for each subscription, 
-- except when start_date is missing
-- using order_date that start_date and end_date to impute in order to keep all subscriptions in table


------------------------------------------
-- DATE-RELATED BUSINESS RULE VALIDATION/EXPLORATION 
------------------------------------------

-- are subscriptions always continuous? or are there sometimes pauses between them?
    -- despite the task definition at hand,
    -- I think it's important to think about the possibility of reactivations,
    -- i.e., customers that left for a certain period of time and then came back to get a new subscription
    -- if we treat them as new customers, we are skewing our insights and losing valuable information


-- handle order duplications 
WITH deduped AS (
    SELECT 
      DISTINCT *
    FROM {{ source("ninox", "orders") }}
),

-- impute order_date as start_date; calculate end_date
joined AS (
    SELECT
        s.customer_id,
        s.subscription_id,
        DATE_TRUNC(COALESCE(s.start_date, DATE(d.order_date)), MONTH) AS start_date, -- order_date is DATETIME
        DATE_TRUNC(COALESCE(s.end_date, DATE_ADD(DATE(d.order_date), INTERVAL 12 MONTH)), MONTH) AS end_date
    FROM deduped AS d
    INNER JOIN {{ source("ninox", "subscriptions") }} AS s
        USING (subscription_id)
),

-- find end of previous customer's subscription
previous AS (
    SELECT
        *,
        LAG(end_date) OVER (PARTITION BY customer_id ORDER BY start_month) AS prev_end_date
    FROM joined
)

-- compare "next" start date with previous end date
SELECT *
FROM previous
WHERE 
    start_date != prev_end_date
ORDER BY customer_id, start_date
-- no rows returned, meaning we have no reactivations


-- can a customer have any simultaneous subscriptions?
    -- this test is accomplished using the same query as above, just changing WHERE to be more specific:
    -- WHERE start_date < prev_end_date
    -- but because the previous query already returned no rows, I won't be writing it out here
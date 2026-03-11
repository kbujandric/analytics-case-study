SELECT
    customer_id,
    subscription_id,
    LOWER(plan_name) AS plan_name,
    number_of_licenses,
    start_date,
    end_date
FROM {{ source("ninox", "subscriptions") }}
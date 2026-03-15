SELECT
    customer_id,
    subscription_id,
    start_date,
    end_date
FROM {{ source("ninox", "subscriptions") }}
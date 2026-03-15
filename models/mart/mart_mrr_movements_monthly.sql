SELECT
    month,
    SUM(start_mrr) AS mrr_start_of_period,
    SUM(mrr_change) AS mrr_change,
    SUM(end_mrr) AS mrr_end_of_period,
    SUM(CASE WHEN change_type = 'new' THEN mrr_change END) AS mrr_new,
    SUM(CASE WHEN change_type = 'expansion' THEN mrr_change END) AS mrr_expansion,
    SUM(CASE WHEN change_type = 'contraction' THEN mrr_change END) AS mrr_contraction,
    SUM(CASE WHEN change_type = 'lost' THEN mrr_change END) AS mrr_lost
FROM {{ ref("int_mrr_movements") }}
WHERE change_type IN ("new", "expansion", "contraction", "lost")
GROUP BY month
ORDER BY month
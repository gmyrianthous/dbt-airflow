{{
    config(
        tags=['daily']
    )
}}
SELECT
    store_id,
    COUNT(*) AS total_customers
FROM
    {{ ref('customer_base') }}
GROUP BY
    1

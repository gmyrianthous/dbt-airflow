{{
    config(
        materialized='table',
    )
}}

SELECT
    payment_date,
    amount,
    SUM(amount) OVER (ORDER BY payment_date)
FROM
    {{ ref('int_revenue_by_date') }}
ORDER BY
    payment_date
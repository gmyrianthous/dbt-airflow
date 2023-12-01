{{
    config(
        tags=['hourly', 'finance']
    )
}}
SELECT
    DATE(payment_date) AS payment_date,
    SUM(amount) AS amount
FROM
    {{ ref('stg_payment') }}
GROUP BY
    1
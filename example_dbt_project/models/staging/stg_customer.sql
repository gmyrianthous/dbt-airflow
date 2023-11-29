{{
    config(
        materialized='table',
        tags=["hourly"],
    )
}}
SELECT
    *
FROM
    {{ source('public', 'customer') }}
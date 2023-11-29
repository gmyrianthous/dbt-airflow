{{
    config(
        materialized='table',
        tags=["finance"],
    )
}}
SELECT
    *
FROM
    {{ source('public', 'store') }}
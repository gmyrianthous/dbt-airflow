SELECT
    *
FROM
    {{ source('public', 'payment') }}

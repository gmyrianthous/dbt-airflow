{{
    config(
        tags=['hourly']
    )
}}

with customer_rentals as (
    select
        customer_id,
        min(rental_date) as first_rental_date,
        max(rental_date) as most_recent_rental_date,
        count(rental_id) as number_of_rentals

    from {{ ref('stg_rental') }}

    group by 1
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    r.first_rental_date,
    r.most_recent_rental_date,
    coalesce(cr.number_of_rentals, 0) as number_of_rentals

from {{ ref('stg_customer') }} c

left join customer_rentals cr ON c.customer_id=cr.customer_id
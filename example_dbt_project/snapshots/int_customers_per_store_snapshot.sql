{% snapshot int_stock_balances_daily_grouped_by_day_snapshot %}

    {{
        config(
          target_schema='public',
          strategy='check',
          check_cols=['total_customers'],
          unique_key='store_id',
          invalidate_hard_deletes=True
        )
    }}

    SELECT
        store_id,
        total_customers,
        CURRENT_DATE
    FROM
        {{ ref('int_customers_per_store') }}

{% endsnapshot %}

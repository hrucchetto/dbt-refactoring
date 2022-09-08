with source as (
    select *
    from {{ source('jaffle_shop', 'customers') }}
),

transformed as (
    select
        id customer_id,
        first_name customer_first_name,
        last_name customer_last_name,
       first_name || ' ' || last_name full_name
    from source
)

select * from transformed
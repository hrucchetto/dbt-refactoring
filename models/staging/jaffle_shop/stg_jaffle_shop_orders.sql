with source as (
    select * 
    from {{ source('jaffle_shop', 'orders') }}
),

transformed as (
    select
        id order_id,
        user_id customer_id,
        order_date order_placed_at,
        status order_status 
    from source
)

select * from transformed
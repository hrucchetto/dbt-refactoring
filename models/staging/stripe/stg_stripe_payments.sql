with source as (
    select *
    from {{ source('stripe', 'payment') }}
),

transformed as (
    select
        id payment_id,
        orderid order_id,
        created payment_created_at,
        status payment_status,
        round(amount / 100.0, 2) payment_amount 
    from source
)

select * from transformed
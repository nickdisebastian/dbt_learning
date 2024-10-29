with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),


payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

completed_orders as (
select
    order_id,
    max(payment_created_at) as payment_finalized_date,
    sum(payment_amount) as total_amount_paid
from payments
where payment_status <> 'fail'
group by 1
),

paid_orders as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        completed_orders.total_amount_paid,
        completed_orders.payment_finalized_date,
    from orders
    left join completed_orders on orders.order_id = completed_orders.order_id
)

select * from paid_orders
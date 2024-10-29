--- Import CTEs
with orders as (
    select * from {{ source('jaffle_shop', 'orders') }}
),

customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

paymemnts as (
    select * from {{ source('stripe', 'payments') }}
),

-- transform subqueries to intermediate CTEs
completed_orders as (
select
    orderid as order_id,
    max(created) as payment_finalized_date,
    sum(amount) / 100.0 as total_amount_paid
from paymemnts
where status <> 'fail'
group by 1
),

-- legacy code
paid_orders as (
    select
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        completed_orders.total_amount_paid,
        completed_orders.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join completed_orders on orders.id = completed_orders.order_id
    left join customers on orders.user_id = customers.id
),


final as (
    select
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.order_placed_at,
        paid_orders.order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        paid_orders.customer_first_name,
        paid_orders.customer_last_name,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
        case 
            when (
                rank() over (
                    partition by paid_orders.customer_id 
                    order by paid_orders.order_placed_at, paid_orders.order_id) 
                = 1)
                then 'new' 
                else 'return' end as nvsr,
        sum(paid_orders.total_amount_paid) over (
            partition by paid_orders.customer_id) as customer_lifetime_value,
        first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
            ) as fdos
from paid_orders)

select * from final 

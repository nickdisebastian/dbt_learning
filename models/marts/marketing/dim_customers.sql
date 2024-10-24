with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select 
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        sum(p.amount) as order_total
     from {{ ref('stg_jaffle_shop__orders') }} o
     left join {{ ref('stg_stripe__payments') }} p on o.order_id = p.order_id
     group by 1,2,3,4

),


customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(order_total) as lifetime_value

    from orders

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_value,0) as lifetime_value

    from customers

    left join customer_orders using (customer_id)

)

select * from final

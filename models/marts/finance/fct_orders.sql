select 
    c.order_id,
    c.customer_id,
    p.amount
from {{ ref('stg_jaffle_shop__orders') }} c
LEFT JOIN {{ ref('stg_stripe__payments') }} p on c.order_id = p.order_id
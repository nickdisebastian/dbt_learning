select 
    ID as PAYMENT_ID,
    ORDERID as ORDER_ID, 
    PAYMENTMETHOD as PAYMENT_METHOD, 
    STATUS, 
    round(AMOUNT/100,2) as AMOUNT, 
    CREATED
from {{ source('stripe', 'payment') }}
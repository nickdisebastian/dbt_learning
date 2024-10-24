select 
    ID as PAYMENT_ID,
    ORDERID as ORDER_ID, 
    PAYMENTMETHOD as PAYMENT_METHOD, 
    STATUS, 
    AMOUNT, 
    CREATED
from {{ source('stripe', 'payment') }}
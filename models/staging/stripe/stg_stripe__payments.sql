select 
    ID as PAYMENT_ID,
    ORDERID as ORDER_ID, 
    PAYMENTMETHOD as PAYMENT_METHOD, 
    STATUS, 
    {{cents_to_dollars('amount')}} as AMOUNT, 
    CREATED
from {{ source('stripe', 'payment') }}
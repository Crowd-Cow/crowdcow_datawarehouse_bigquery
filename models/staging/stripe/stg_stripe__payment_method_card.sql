with 
source as ( select * from {{ source('stripe', 'payment_method_card') }} )

select
    payment_method_id
    ,{{ clean_strings('brand') }} as brand
    ,{{ clean_strings('funding') }} as funding_type
    ,{{ clean_strings('wallet_type') }} as wallet_type
from source
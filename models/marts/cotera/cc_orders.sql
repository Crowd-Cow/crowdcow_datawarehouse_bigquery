with

order_details as ( select * from {{ ref('orders') }} where not is_rastellis or is_rastellis is null )

select *
from order_details
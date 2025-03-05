with

event_groupings as ( select * from {{ ref('events') }} where event_name in ('ORDER_ADD_TO_CART','PRODUCT_CARD_QUICK_ADD_TO_CART','ORDER_COMPLETE'))

select 
    distinct visit_id
    ,countif(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0 as add_to_carts
    ,countif(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0
    and countif(event_name = 'ORDER_COMPLETE')  > 0 as order_complete
from event_groupings
group by 1


with

event_groupings as ( select * from {{ ref('events') }} )

select distinct event_groupings.visit_id
    ,count_if(event_groupings.event_type = 'VIEWED SLIDEOUT CART') > 0 and count_if(event_groupings.event_type = 'VIEWED ADDRESS PAGE') > 0 as slideout_and_address_page
    ,count_if(event_groupings.event_type = 'VIEWED SLIDEOUT CART') > 0 and count_if(event_groupings.event_type = 'VIEWED ADDRESS PAGE') > 0 and count_if(event_groupings.event_type = 'VIEWED PAYMENT PAGE') > 0 as address_page_and_payment_page
    ,count_if(event_groupings.event_type = 'VIEWED SLIDEOUT CART') > 0 and count_if(event_groupings.event_type = 'VIEWED ADDRESS PAGE') > 0 and count_if(event_groupings.event_type = 'VIEWED PAYMENT PAGE') > 0 and count_if(event_groupings.event_type = 'CHECKOUT COMPLETE') > 0 as payment_page_and_checkout_complete
from event_groupings
group by 1
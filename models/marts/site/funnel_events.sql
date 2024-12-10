with

event_groupings as ( select * from {{ ref('events') }} )

select distinct event_groupings.visit_id
    ,countif( (event_groupings.category = 'CART' and event_groupings.action = 'VIEW') or (event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url = 'HTTPS://WWW.CROWDCOW.COM/SHOPPING_CART') ) > 0 
        and countif(event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url like '%/O%/DELIVERY%') > 0 
        as slideout_and_address_page
    ,countif( (event_groupings.category = 'CART' and event_groupings.action = 'VIEW') or (event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url = 'HTTPS://WWW.CROWDCOW.COM/SHOPPING_CART') ) > 0 
        and countif(event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url like '%/O%/DELIVERY%') > 0 
        and countif(event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url like '%/O%/PAYMENT%') > 0 
        as address_page_and_payment_page
    ,countif( (event_groupings.category = 'CART' and event_groupings.action = 'VIEW') or (event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url = 'HTTPS://WWW.CROWDCOW.COM/SHOPPING_CART') ) > 0 
        and countif(event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url like '%/O%/DELIVERY%') > 0 
        and countif(event_groupings.event_name = 'PAGE_VIEW' and event_groupings.url like '%/O%/PAYMENT%') > 0  
        and countif(event_groupings.event_name = 'ORDER_COMPLETE') > 0 
        as payment_page_and_checkout_complete
from event_groupings
group by 1
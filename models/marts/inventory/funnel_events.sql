with

events as ( select * from {{ ref('events') }} )

,viewed_slideout_cart as (
    select visit_id
    from events
    where event_type = 'VIEWED SLIDEOUT CART'
)

,viewed_address as (
select visit_id
    from events
    where event_type = 'VIEWED ADDRESS PAGE'
)

,viewed_payment as (
select distinct visit_id
    from events
    where event_type = 'VIEWED PAYMENT PAGE'
)

,checkout_complete as (
select visit_id
    from events
    where event_type = 'CHECKOUT COMPLETE'
)

select distinct events.visit_id
    ,case when viewed_slideout_cart.visit_id is not null and viewed_address.visit_id is not null then 1 else 0 end as SLIDEOUT_AND_ADDRESS_PAGE
    ,case when viewed_address.visit_id is not null and viewed_payment.visit_id is not null then 1 else 0 end as ADDRESS_PAGE_AND_PAYMENT_PAGE
    ,case when viewed_payment.visit_id is not null and checkout_complete.visit_id is not null then 1 else 0 end as PAYMENT_AND_CHECKOUT_COMPLETE
from events
left join viewed_slideout_cart on events.visit_id = viewed_slideout_cart.visit_id
left join viewed_address on events.visit_id = viewed_address.visit_id
left join viewed_payment on events.visit_id = viewed_payment.visit_id
left join checkout_complete on events.visit_id = checkout_complete.visit_id

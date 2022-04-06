{{
  config(
        materialized = 'incremental',
        unique_key = 'event_id',
    )
}}


with

events as ( select * from {{ ref('stg_cc__events') }}   

    {% if is_incremental() %}
      where occurred_at_utc >= coalesce((select max(occurred_at_utc) from {{ this }}), '1900-01-01')
    {% endif %})

,event_details as (
    select event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc
        ,updated_at_utc
        ,event_sequence_number
        ,event_name
        ,category
        ,action
        ,label
        ,experiments
        ,is_member
        ,token
        ,order_id
        ,url
        ,referrer_url
        ,subscription_id
        ,title
        ,old_scheduled_arrival_date
        ,new_scheduled_arrival_date
        ,old_scheduled_fulfillment_date
        ,new_scheduled_fulfillment_date
        ,reason
        ,user_making_change_id
        ,case when event_name = 'VIEWED_PRODUCT' then 'PDP VIEW'
              when category = 'PRODUCT' and action = 'PAGE-INTERACTION' and label = 'CLICKED-ADD-TO-CART' then 'PDP ADD TO CART'
              when category = 'CHECKOUT' and action = 'VIEWED-UPSELL-CAROUSEL-PRODUCT-CARD' then 'VIEWED PDC UPSELL CAROUSEL'
              when category = 'CHECKOUT' and action = 'PAGE-INTERACTION' and label = 'VIEWED-UPSELL-CAROUSEL' then 'VIEWED CHECKOUT UPSELL CAROUSEL'
              when category = 'PRODUCT' and action = 'CART-UPSELL-QUICK-ADD' then 'UPSELL QUICK ADD FROM CAROUSEL'
              when event_name = 'ORDER_COMPLETE' then 'CHECKOUT COMPLETE'
              when event_name = 'UNSUBSCRIBED' then 'CANCELLED MEMBERSHIP'
              when event_name = 'SUBSCRIBED' then 'CREATED MEMBERSHIP'
              when category = 'CART' and action = 'VIEW' then 'VIEWED SLIDEOUT CART'
              when category = 'CHECKOUT' and action = 'REACHED-STEP' and label = '1' then 'CLICKED CHECKOUT'
              when category = 'CHECKOUT' and action = 'REACHED-STEP' and label = '2' then 'CLICKED CONTINUE TO PAYMENT'
              when category = 'CHECKOUT' and action = 'REACHED-STEP' and label = '3' then 'CLICKED PLACE ORDER'
              when event_name = 'PAGE_VIEW' and url like '%/O%/DELIVERY%' then 'VIEWED ADDRESS PAGE'
              when event_name = 'PAGE_VIEW' and url like '%/O%/PAYMENT%' then 'VIEWED PAYMENT PAGE'
              when event_name = 'ORDER_ENTER_PAYMENT' then 'PAYMENT INFO ENTERED'
              when category = 'ERROR' and action = 'ADDRESS-ERROR' then 'ADDRESS ERROR'
              when event_name = 'PRODUCT CARD VIEWED' then 'PDC VIEW'
              when event_name = 'PRODUCT CARD CLICKED' then 'PDC CLICK'
              when label = 'CLICKED-ADD-TO-CART-PRODUCT-CARD' then 'PDC ADD TO CART' 
              when event_name = 'CHECKOUT_ADD-PAYMENT-INFO' then 'PAYMENT INFO ADDED'
              else null
              end as event_type
    from events
)

select *
from event_details
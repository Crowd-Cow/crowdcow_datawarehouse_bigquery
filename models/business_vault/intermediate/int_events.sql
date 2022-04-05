{{
  config(
        materialized = 'incremental',
        unique_key = 'event_id',
    )
}}


with

events as ( select * from {{ ref('stg_cc__events') }} )

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
        ,token as order_token
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
              when event_name = 'PAGE_VIEW' then 'HOMEPAGE VIEW'
              when category = 'PRODUCT' and action = 'PAGE-INTERACTION' and label = 'CLICKED-ADD-TO-CART' then 'PDP ADD TO CART'
              when category = 'CHECKOUT' and action = 'VIEWED-UPSELL-CAROUSEL-PRODUCT-CARD' then 'VIEWED PDC UPSELL CAROUSEL'
              when category = 'CHECKOUT' and action = 'PAGE-INTERACTION' and label = 'VIEWED-UPSELL-CAROUSEL' then 'VIEWED CHECKOUT UPSELL CAROUSEL'
              when category = 'CHECKOUT' and action = 'REACHED-STEP' then 'CHECKOUT BUTTON CLICK IN CART'
              when event_name = 'ORDER_COMPLETE' then 'CHECKOUT COMPLETE'
              when event_name = 'UNSUBSCRIBED' then 'UNSUBSCRIBED'
              when event_name = 'PAGE_VIEW' and url like '%/O%/DELIVERY%' then 'VIEWED ADDRESS PAGE'
              when event_name = 'PAGE_VIEW' and URL like '%/O%/PAYMENT%' then 'VIEWED PAYMENT PAGE'
              when event_name = 'ORDER_ENTER_PAYMENT' then 'PAYMENT INFO ENTERED'
              when category = 'ERROR' and action = 'ADDRESS-ERROR' then 'ADDRESS ERROR'
              when event_name = 'PRODUCT_CARD_VIEWED' then 'PDC VIEW'
              when event_name = 'CHECKOUT_ADD-PAYMENT-INFO' then 'PAYMENT INFO ADDED'
              else null
              end as event_type
    from events
)

select *
from event_details
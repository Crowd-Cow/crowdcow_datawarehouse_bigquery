{{
  config(
    tags = ["intermediate"]
  )
}}

with 

event_visits as ( select * from {{ ref('stg_cc__event_visits') }} )
,event_checkout_initiated as ( select * from {{ ref('stg_cc__event_checkout_initiated') }} )
,event_checkout_payment_selected as ( select * from {{ ref('stg_cc__event_checkout_payment_selected') }} )
,event_order_complete as ( select * from {{ ref('stg_cc__event_order_complete') }} )
,event_order_paid as ( select * from {{ ref('stg_cc__event_order_paid') }} )
,event_page_view as ( select * from {{ ref('stg_cc__event_page_view') }} )
,event_pdp_added_to_cart as ( select * from {{ ref('stg_cc__event_pdp_added_to_cart') }} )
,event_viewed_product as ( select * from {{ ref('stg_cc__event_viewed_product') }} )
,event_click_navigation as ( select * from {{ ref('stg_cc__event_click_navigation') }} )

,visit_events as (
    select
        visit_id
        ,user_id
        ,null as event_id
        ,'visit_start' as event_name
        ,started_at_utc as occurred_at_utc
    from stg_cc__event_visits
)
,checkout_initiated as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'checkout_initiated' as event_name
        ,occurred_at_utc
    from stg_cc__event_checkout_initiated
)
,checkout_payment_selected as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'checkout_payment_selected' as event_name
        ,occurred_at_utc
    from stg_cc__event_checkout_payment_selected
)
,order_complete as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'order_complete' as event_name
        ,occurred_at_utc
    from stg_cc__event_order_complete
)
,order_paid as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'order_paid' as event_name
        ,occurred_at_utc
    from stg_cc__event_order_paid
)
,page_view as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'page_view: ' ||
            split_part(parse_url(page_viewed_url):path::text,'/',1) as event_name
        ,occurred_at_utc
    from stg_cc__event_page_view
)
,pdp_added_to_cart as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'pdp_added_to_cart' as event_name
        ,occurred_at_utc
    from stg_cc__event_pdp_added_to_cart
)
,viewed_product as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'viewed_product: ' || bid_item_name as event_name
        ,occurred_at_utc
    from stg_cc__event_viewed_product
)
,click_navigation as (
    select
        visit_id
        ,user_id
        ,event_id
        ,'click_navigation: ' || navigation_label as event_name
        ,occurred_at_utc
    from stg_cc__event_click_navigation
)
,union_events as (
    select * from visit_events
    union all
    select * from checkout_initiated
    union all
    select * from checkout_payment_selected
    union all
    select * from order_complete
    union all
    select * from order_paid
    union all
    select * from page_view
    union all
    select * from pdp_added_to_cart
    union all
    select * from viewed_product
    union all
    select * from click_navigation
)

select * from union_events
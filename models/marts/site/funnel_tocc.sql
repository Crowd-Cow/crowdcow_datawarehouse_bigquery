with

events as ( select * from {{ ref('events') }} where occurred_at_utc >= '2024-11-21' )
,visits as ( 
    select * 
    from {{ ref('visits') }} 
    where (visit_landing_page_path = '/TOC' or visit_landing_page_path = '/')
    and is_prospect
    and not is_bot
    and not is_internal_traffic
    and tocc_redirect = 'EXPERIMENTAL1.0' )

,funnel_lp_cart as (
select
    distinct 
    visits.visit_id
    ,visits.visitor_ip_session
    ,"funnel_lp_cart" as funnel
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
        as LP
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B')) = 0
         as CART
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B')) = 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
        as initiate_checkout
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B')) = 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
        as ORDER_ENTER_ADDRESS
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B')) = 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
        as ORDER_ENTER_PAYMENT
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B')) = 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
        as ORDER_COMPLETE
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') = '/TASTE-OF-CROWD-COW-B')) = 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
    and COUNTIF(event_name = 'ORDER_PAID') > 0
        as ORDER_PAID
FROM visits
left join events on visits.visit_id = events.visit_id
group by 1,2,3

)

,funnel_lp_tocpdp_cart as (

select
    distinct 
    visits.visit_id
    ,visits.visitor_ip_session
    ,"funnel_lp_tocpdp_cart" as funnel
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
        as LP
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
        as TOCC_PDP
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
        as CART
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
        as initiate_checkout
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
        as ORDER_ENTER_ADDRESS
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
        as ORDER_ENTER_PAYMENT
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
        as ORDER_COMPLETE
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
    and COUNTIF(event_name = 'ORDER_PAID') > 0
        as ORDER_PAID

FROM  visits
left join events on visits.visit_id = events.visit_id
group by 1,2,3
)

,funnel_lp_tocpdp_pdps_cart as (
select
    distinct 
    visits.visit_id
    ,visits.visitor_ip_session
    ,"funnel_lp_tocpdp_pdps_cart" as funnel
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
        as LP
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
        as TOCC_PDP
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%') > 0
        as PDPs
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
        as CART
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
        as initiate_checkout
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
        as ORDER_ENTER_ADDRESS
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
        as ORDER_ENTER_PAYMENT
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
        as ORDER_COMPLETE
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/PRODUCTS/TASTE-CROWD-COW%' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)')= '/TASTE-OF-CROWD-COW-B') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%') > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
    and COUNTIF(event_name = 'ORDER_PAID') > 0
        as ORDER_PAID

FROM visits
left join events on visits.visit_id = events.visit_id
group by 1,2,3
)

,funnel_lp_pdps_cart as (
select
    distinct 
    visits.visit_id
    ,visits.visitor_ip_session
    ,"funnel_lp_pdps_cart" as funnel
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
        as LP
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
     and COUNTIF(event_name = 'PAGE_VIEW' and (on_page_path != '')) > 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND (on_page_path like '/PRODUCTS/TASTE-CROWD-COW%') ) = 0
        as PDPs
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
     and COUNTIF(event_name = 'PAGE_VIEW' and (on_page_path != '')) > 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND (on_page_path like '/PRODUCTS/TASTE-CROWD-COW%') ) = 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
        as CART
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
     and COUNTIF(event_name = 'PAGE_VIEW' and (on_page_path != '')) > 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND (on_page_path like '/PRODUCTS/TASTE-CROWD-COW%') ) = 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
        as initiate_checkout
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
     and COUNTIF(event_name = 'PAGE_VIEW' and (on_page_path != '')) > 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND (on_page_path like '/PRODUCTS/TASTE-CROWD-COW%') ) = 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
        as ORDER_ENTER_ADDRESS
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
     and COUNTIF(event_name = 'PAGE_VIEW' and (on_page_path != '')) > 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND (on_page_path like '/PRODUCTS/TASTE-CROWD-COW%') ) = 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
        as ORDER_ENTER_PAYMENT
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
     and COUNTIF(event_name = 'PAGE_VIEW' and (on_page_path != '')) > 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND (on_page_path like '/PRODUCTS/TASTE-CROWD-COW%') ) = 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
        as ORDER_COMPLETE
    ,COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path = '/TASTE-OF-CROWD-COW-B') > 0
     and COUNTIF(event_name = 'PAGE_VIEW' and (on_page_path != '')) > 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND (on_page_path like '/PRODUCTS/TASTE-CROWD-COW%') ) = 0
     and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '/ORDER/%' AND (not REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/PRODUCTS/TASTE-CROWD-COW%')) > 0
    and COUNTIF(event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY' AND REGEXP_EXTRACT(REFERRER_URL, r'^HTTPS?://[^/]+([^?#]*)') like '/ORDER/%') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
    and COUNTIF(event_name = 'ORDER_PAID') > 0
        as ORDER_PAID
FROM visits
left join events on visits.visit_id = events.visit_id
group by 1,2,3
)


select 
    visit_id
    ,visitor_ip_session
    ,funnel
    ,LP
    ,null as TOCC_PDP
    ,null as PDPs
    ,CART
    ,initiate_checkout
    ,ORDER_ENTER_ADDRESS
    ,ORDER_ENTER_PAYMENT
    ,ORDER_COMPLETE
    ,ORDER_PAID
from funnel_lp_cart
union all
select 
    visit_id
    ,visitor_ip_session
    ,funnel
    ,LP
    ,TOCC_PDP
    ,null as PDPs
    ,CART
    ,initiate_checkout
    ,ORDER_ENTER_ADDRESS
    ,ORDER_ENTER_PAYMENT
    ,ORDER_COMPLETE
    ,ORDER_PAID
from funnel_lp_tocpdp_cart
union all
select 
    visit_id
    ,visitor_ip_session
    ,funnel
    ,LP
    ,TOCC_PDP
    ,PDPs
    ,CART
    ,initiate_checkout
    ,ORDER_ENTER_ADDRESS
    ,ORDER_ENTER_PAYMENT
    ,ORDER_COMPLETE
    ,ORDER_PAID
from funnel_lp_tocpdp_pdps_cart
union all 
select 
    visit_id
    ,visitor_ip_session
    ,funnel
    ,LP
    ,null as TOCC_PDP
    ,PDPs
    ,CART
    ,initiate_checkout
    ,ORDER_ENTER_ADDRESS
    ,ORDER_ENTER_PAYMENT
    ,ORDER_COMPLETE
    ,ORDER_PAID
from funnel_lp_pdps_cart




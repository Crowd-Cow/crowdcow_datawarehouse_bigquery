{{ config(
    materialized='incremental',
    unique_key='visit_id', 
    partition_by={'field': 'max_ocurred_event', 'data_type': 'timestamp'},
    cluster_by=['visit_id']
) }}

with events as (
    select * from {{ ref('stg_cc__events') }}
    {% if is_incremental() %}
      where occurred_at_utc > (select max(max_ocurred_event) from {{ this }})
    {% endif %}
)

,visit_activity as (
    select 
        visit_id,
        MIN(occurred_at_utc) as min_ocurred_event,
        max(occurred_at_utc) as max_ocurred_event,
        COUNT(DISTINCT IF(event_name = 'SUBSCRIBED', subscription_id, NULL)) AS subscribes,
        COUNT(DISTINCT IF(event_name = 'UNSUBSCRIBED', subscription_id, NULL)) AS unsubscribes,
        COUNTIF(event_name = 'SIGN_UP') AS sign_ups,
        COUNTIF(event_name = 'ORDER_COMPLETE') AS order_completes,
        COUNTIF(category = 'PRODUCT' AND action = 'VIEW-IMPRESSION') AS pcp_impressions,
        COUNTIF(category = 'PRODUCT' AND action = 'IMPRESSION-CLICK') AS pcp_impression_clicks,
        COUNTIF(category = 'PRODUCT' AND action = 'PAGE-INTERACTION' AND label = 'CLICKED-ADD-TO-CART') AS pdp_add_to_carts,
        COUNTIF(event_name = 'VIEWED_PRODUCT') AS viewed_pdps,
        COUNTIF(event_sequence_number = 1 AND event_name = 'PAGE_VIEW' AND REGEXP_CONTAINS(url, r'^$|^L$')) AS homepage_views,
        COUNTIF(event_name = 'CLICK' AND label IN ('GET STARTED', 'CLAIM OFFER') AND on_page_path LIKE '%/LANDING%') AS landing_offer_claim,
        COUNTIF(event_name = 'CLICK' AND label = 'SKIP' AND on_page_path LIKE '%/LANDING%') AS landing_offer_skipped,
        COUNTIF(event_name = 'ORDER_ADD_TO_CART' or event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART') as add_to_carts,
        COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') as begin_checkout,
        COUNTIF(event_name = 'PAGE_VIEW') as page_views,
        COUNTIF(event_name = 'CLICK') as clicks,
        COUNTIF(event_name = 'SCROLL_DEPTH' and SCROLL_DEPTH >= 25 ) as scroll_depth_25,
        COUNT(DISTINCT IF(event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-home_page_redirect') = 'experimental', visit_id, NULL)) AS home_page_redirect_experimental,
        COUNT(DISTINCT IF(event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-home_page_redirect') = 'control', visit_id, NULL)) AS home_page_redirect_control,
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-hp_redirect_2') = 'experimental'  THEN visit_id END) AS hp_redirect_2_experimental,
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-hp_redirect_2') = 'control'  THEN visit_id END) AS hp_redirect_2_control, 
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-express_checkout') = 'control'  THEN visit_id END) AS express_checkout_control,
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-express_checkout') = 'experimental'  THEN visit_id END) AS express_checkout_experimental, 
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-taste_of_crowd_cow_redirect') = 'control'  THEN visit_id END) AS tocc_redirect_control,
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-taste_of_crowd_cow_redirect') = 'experimental'  THEN visit_id END) AS tocc_redirect_experimental, 
        COUNT(*) AS event_count
    from events
    group by 1
)

select * from visit_activity
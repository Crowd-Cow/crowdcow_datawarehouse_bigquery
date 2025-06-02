with

events as ( select visit_id, event_id, event_name, event_sequence_number, on_page_path  from {{ ref('events') }})
,visits as ( 
    select visit_id, visitor_ip_session
    from {{ ref('visits') }} 
    where 
    not is_proxy
    and not is_server
    and not is_internal_traffic ) 

select 
    distinct 
    visits.visit_id
    ,visits.visitor_ip_session
    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0  as session_start

    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0
    and COUNTIF( event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED' ) > 0 as viewed_product      

    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0
    and COUNTIF( event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED' ) > 0
    and COUNTIF(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0 as add_to_carts
    
    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0
    and COUNTIF( event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED' ) > 0
    and COUNTIF(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0 
    and COUNTIF((event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY') or (event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERIES') ) > 0 as initiate_checkout
    
    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0
    and COUNTIF( event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED' ) > 0
    and COUNTIF(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0 
    and COUNTIF((event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY') or (event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERIES')) > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0 as ORDER_ENTER_ADDRESS
    
    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0
    and COUNTIF( event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED' ) > 0
    and COUNTIF(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0 
    and COUNTIF((event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY') or (event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERIES')) > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0  as ORDER_ENTER_PAYMENT
    
    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0
    and COUNTIF( event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED' ) > 0
    and COUNTIF(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0 
    and COUNTIF((event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY') or (event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERIES')) > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0 as ORDER_COMPLETE

    ,COUNTIF(event_sequence_number = 1 and event_name = 'PAGE_VIEW') > 0
    and COUNTIF( event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED' ) > 0
    and COUNTIF(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'ORDER_ADD_TO_CART') > 0          
    and COUNTIF((event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERY') or (event_name = 'PAGE_VIEW' AND on_page_path like '%/DELIVERIES')) > 0
    and COUNTIF(event_name = 'ORDER_ENTER_ADDRESS') > 0
    and COUNTIF(event_name = 'ORDER_ENTER_PAYMENT') > 0
    and COUNTIF(event_name = 'ORDER_COMPLETE') > 0
    and COUNTIF(event_name = 'ORDER_PAID') > 0 as ORDER_PAID

from visits
left join events on visits.visit_id = events.visit_id
group by 1,2

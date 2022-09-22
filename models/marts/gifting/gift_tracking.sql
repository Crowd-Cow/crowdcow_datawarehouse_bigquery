with

orders as ( select * from {{ ref('orders') }} where is_paid_order and not is_cancelled_order )
,gift_info as ( select * from {{ ref('stg_cc__gift_infos') }} )
,shipments as (select * from {{ ref('shipments') }} )
,users as ( select * from {{ ref('users') }} )
,fcs as ( select * from {{ ref('fcs') }} )

,gift_order_details as (
    select 
        orders.order_id
        ,order_token
        ,fc_name
        ,users.user_email as gifter_email
        ,recipient_name as gift_recipient_name
        ,recipient_email as gift_recipient_email
        ,net_product_revenue
        ,is_gift_order
        ,is_bulk_gift_order
        ,is_gift_card_order
        ,orders.order_paid_at_utc
        ,order_scheduled_fulfillment_date_utc
        ,order_scheduled_arrival_date_utc
        ,shipments.shipment_token
        ,shipments.shipped_at_utc
        ,shipments.original_est_delivery_date_utc
        ,shipments.est_delivery_date_utc
        ,shipments.delivered_at_utc
    from orders
        left join shipments on shipments.order_id = orders.order_id
        left join gift_info on orders.order_id = gift_info.order_id
        left join users on orders.user_id = users.user_id
        left join fcs on orders.fc_key = fcs.fc_key
    where gift_info.order_id is not null
)

select *
from gift_order_details
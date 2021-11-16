with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,shipping_credit as ( select * from {{ ref('stg_cc__credits') }} )

,gift_card as (
    select
        distinct gift_info_id
    from stg_cc__gift_cards
    where gift_info_id is not null
)

,gift_info as (
    select distinct
        order_id
        ,gift_card.gift_info_id is not null as is_gift_card
    from stg_cc__gift_infos
        left join gift_card on stg_cc__gift_infos.gift_info_id = gift_card.gift_info_id
)

,has_shipping_credit as (
    select distinct
        order_id
    from shipping_credit
    where credit_type = 'FREE SHIPPING'
)

,flags as (
    select 
        orders.order_id
        ,orders.user_id
        ,orders.subscription_id
        ,orders.order_created_at_utc
        ,orders.subscription_id is null as is_ala_carte_order
        ,orders.subscription_id is not null as is_membership_order
        ,orders.order_checkout_completed_at_utc is not null as is_completed_order
        ,orders.order_paid_at_utc is not null as is_paid_order
        ,orders.order_cancelled_at_utc is not null as is_cancelled_order
        ,orders.order_checkout_completed_at_utc is null and orders.order_cancelled_at_utc is not null as is_abandonded_order
        ,has_shipping_credit.order_id is not null as has_free_shipping
        ,gift_info.order_id is not null and not gift_info.is_gift_card and orders.parent_order_id is null and orders.order_type <> 'BULK ORDER' as is_gift_order
        ,gift_info.order_id is not null and not gift_info.is_gift_card and (orders.parent_order_id is not null or orders.order_type = 'BULK ORDER') as is_bulk_gift_order
        ,gift_info.order_id is not null and gift_info.is_gift_card as is_gift_card_order
    from orders
        left join gift_info on orders.order_id = gift_info.order_id 
        left join has_shipping_credit on orders.order_id = has_shipping_credit.order_id
)

select *
from flags
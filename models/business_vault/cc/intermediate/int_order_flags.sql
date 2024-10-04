with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,shipping_credit as ( select * from {{ ref('stg_cc__credits') }} )
,shipment as ( select * from {{ ref('stg_cc__shipments') }} )
,bids as (select * from {{ ref('stg_cc__bids') }} )
,gift_cards as (select * from {{ ref('stg_cc__gift_cards') }} )
,gift_infos as ( select * from {{ ref('stg_cc__gift_infos') }} )
,order_reschedule as ( select distinct order_id from {{ ref('stg_cc__events') }} where event_name = 'ORDER_RESCHEDULED' )
,membership_status as (select * from {{ ref('stg_cc__subscriptions') }} )
,user as ( select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null )
,stuck_order_flags as ( select * from {{ ref('stg_cc__subscription_statuses') }} )
,moolah as ( select * from {{ ref('int_order_rewards') }} )
,gift_card_redemption as (select * from {{ ref('credits') }} where credit_business_group = 'GIFT CARD REDEMPTION' )

,distinct_stuck_order_flag as (
    select
        order_id
        ,was_week_out_notification_sent
        ,is_all_inventory_reserved
        ,does_need_customer_confirmation
        ,is_time_to_charge
        ,is_payment_failure
        ,was_referred_to_customer_service
        ,is_invalid_postal_code
        ,can_retry_payment
        ,is_under_order_minimum
        ,is_order_scheduled_in_past
        ,is_order_missing
        ,is_order_cancelled
        ,is_order_charged    
        ,is_bids_fulfillment_at_risk
    from stuck_order_flags
    qualify row_number() over(partition by order_id order by updated_at_utc desc) = 1
)

,gift_card as (
    select
        distinct gift_info_id
    from gift_cards
    where gift_info_id is not null
)

,gift_info as (
    select distinct
        order_id
        ,gift_card.gift_info_id is not null as is_gift_card
    from gift_infos
        left join gift_card on gift_infos.gift_info_id = gift_card.gift_info_id
)

,has_shipping_credit as (
    select distinct
        order_id
    from shipping_credit
    where credit_type = 'FREE_SHIPPING'
)

,shipping_flags as (
    select
        order_id
        ,max(shipped_at_utc) as shipped_at_utc
        ,max(delivered_at_utc) as delivered_at_utc
        ,max(lost_at_utc) as lost_at_utc
    from shipment
    group by 1
)

,fulfillment_risk as (
    select order_id, max(is_fulfillment_at_risk) as is_fulfillment_risk
    from bids
    group by 1
)

,placed_by_uncancelled_member as (
    select distinct order_id
        from orders
        join membership_status on orders.user_id = membership_status.user_id
            and orders.order_paid_at_utc >= membership_status.subscription_created_at_utc
            and (orders.order_paid_at_utc < membership_status.subscription_cancelled_at_utc
                or membership_status.subscription_cancelled_at_utc is null
            )
)
,moolah_orders as (
    select distinct order_id
    from moolah
    where total_moolah_redeemed < 0
)

,gift_card_redemption_orders as (
    select distinct order_id
    from gift_card_redemption

)

,flags as (
    select 
        orders.order_id
        ,orders.user_id
        ,orders.subscription_id
        ,orders.order_created_at_utc
        ,orders.order_checkout_completed_at_utc
        ,orders.order_paid_at_utc
        ,orders.order_cancelled_at_utc

        ,orders.subscription_id is null 
            and orders.parent_order_id is null 
            and orders.order_type <> 'BULK ORDER'
            and user.user_email not like '%CORPGIFTORDERS%' as is_ala_carte_order
        
        ,orders.subscription_id is not null as is_membership_order
        ,orders.order_checkout_completed_at_utc is not null as is_completed_order
        ,orders.order_paid_at_utc is not null as is_paid_order
        ,orders.order_cancelled_at_utc is not null as is_cancelled_order
        ,orders.order_checkout_completed_at_utc is null and orders.order_cancelled_at_utc is not null as is_abandonded_order
        ,has_shipping_credit.order_id is not null as has_free_shipping
        ,gift_info.order_id is not null and not gift_info.is_gift_card and orders.parent_order_id is null and orders.order_type = 'E-COMMERCE' as is_gift_order
        
        ,orders.parent_order_id is not null 
            or orders.order_type = 'BULK ORDER'
            or user.user_email like '%CORPGIFTORDERS%' as is_bulk_gift_order
        
        ,gift_info.order_id is not null and gift_info.is_gift_card as is_gift_card_order
        ,shipping_flags.shipped_at_utc is not null as has_shipped
        ,shipping_flags.delivered_at_utc is not null as has_been_delivered
        ,shipping_flags.lost_at_utc is not null as has_been_lost
        ,coalesce(fulfillment_risk.is_fulfillment_risk,0) as is_fulfillment_risk
        ,order_reschedule.order_id is not null as is_rescheduled
        ,orders.order_type = 'RFG' as is_rastellis
        ,orders.order_type = 'QVC' as is_qvc
        ,placed_by_uncancelled_member.order_id is not null as was_member

        ,distinct_stuck_order_flag.was_week_out_notification_sent
        ,distinct_stuck_order_flag.is_all_inventory_reserved
        ,distinct_stuck_order_flag.does_need_customer_confirmation
        ,distinct_stuck_order_flag.is_time_to_charge
        ,distinct_stuck_order_flag.is_payment_failure
        ,distinct_stuck_order_flag.was_referred_to_customer_service
        ,distinct_stuck_order_flag.is_invalid_postal_code
        ,distinct_stuck_order_flag.can_retry_payment
        ,distinct_stuck_order_flag.is_under_order_minimum
        ,distinct_stuck_order_flag.is_order_scheduled_in_past
        ,distinct_stuck_order_flag.is_order_missing
        ,distinct_stuck_order_flag.is_order_cancelled
        ,distinct_stuck_order_flag.is_order_charged    
        ,distinct_stuck_order_flag.is_bids_fulfillment_at_risk
        ,moolah_orders.order_id is not null as is_moolah_order
        ,gift_card_redemption_orders.order_id is not null as has_gift_card_redemption
    from orders
        left join gift_info on orders.order_id = gift_info.order_id 
        left join has_shipping_credit on orders.order_id = has_shipping_credit.order_id
        left join shipping_flags on orders.order_id = shipping_flags.order_id
        left join fulfillment_risk on orders.order_id = fulfillment_risk.order_id
        left join order_reschedule on cast(orders.order_id as string) = order_reschedule.order_id
        left join placed_by_uncancelled_member on orders.order_id = placed_by_uncancelled_member.order_id
        left join user on orders.user_id = user.user_id
        left join distinct_stuck_order_flag on orders.order_id = distinct_stuck_order_flag.order_id
        left join moolah_orders on orders.order_id = moolah_orders.order_id
        left join gift_card_redemption_orders on orders.order_id = gift_card_redemption_orders.order_id
)

select *
from flags

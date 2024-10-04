with

gift_card as ( select * from {{ ref('gift_cards') }} )
,gift_card_redemption as ( select * from {{ ref('credits') }} where awarded_cow_cash_entry_type = 'GIFT_CARD' )
,gift_card_order_user as ( select order_id,user_id from {{ ref('orders') }} where is_gift_card_order )

,gift_card_purchase as (
    select
        gift_card.gift_card_id
        ,gift_card.gift_info_id
        ,gift_card_order_user.user_id as purchase_user_id
        ,null as redemption_user_id
        ,gift_card.order_id
        ,'GIFT CARD PURCHASE' as entry_type
        ,gift_card.gift_card_amount_usd as amount_usd
        ,gift_card.created_at_utc
        ,gift_card.updated_at_utc
    from gift_card
        left join gift_card_order_user on gift_card.order_id = gift_card_order_user.order_id
)

,union_gift_card_transactions as (
    select
        *
    from gift_card_purchase

    union all
    
    select
        gift_card_id
        ,null as gift_info_id
        ,null as purchase_user_id
        ,user_id as redemption_user_id
        ,order_id
        ,'GIFT CARD REDEMPTION' as entry_type
        ,-credit_discount_usd as amount_usd
        ,created_at_utc
        ,updated_at_utc
    from gift_card_redemption
)

,calc_gift_card_balance as (
    select
        *
        ,round(sum(amount_usd) over(partition by gift_card_id order by created_at_utc),2) as balance
        ,round(sum(amount_usd) over(partition by gift_card_id),2) as current_balance
        ,first_value(order_id) over(partition by gift_card_id order by created_at_utc) as purchase_order_id
        ,first_value(purchase_user_id) over(partition by gift_card_id order by created_at_utc) as purchasing_user_id
        ,min(if(entry_type = 'GIFT CARD PURCHASE',created_at_utc,null)) over(partition by gift_card_id) as gift_card_purchased_at_utc
        ,max(if(entry_type = 'GIFT CARD REDEMPTION',created_at_utc,null)) over(partition by gift_card_id) as last_redemption_at_utc
    from union_gift_card_transactions
    order by gift_card_id,entry_type,created_at_utc
)

,add_flags as (
    select
        *
        ,{{ dbt_utils.surrogate_key( ['gift_card_id','created_at_utc','amount_usd'] ) }} as gift_card_unique_id
        ,current_balance > 0 as has_outstanding_balance
        ,purchase_order_id is null as is_bulk_generated
    from calc_gift_card_balance
)

select * from add_flags

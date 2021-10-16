with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,refunds as ( select * from {{ ref('stg_cc__refunds') }} )
,credits as ( select * from {{ ref('stg_cc__credits') }} )
,order_statements as ( select * from {{ ref('stg_cc__order_statements') }} )
,bids as ( select * from {{ ref('stg_cc__bids') }} )
,flags as ( select * from {{ ref('order_flags') }} )
,ranks as ( select * from {{ ref('order_ranks') }} )

,bid_amounts as (
    select 
        order_id
        ,sum(item_price_usd * bid_quantity) as bid_amount
    from bids
    group by 1
)

,refund_amounts as (
    select
        order_id
        ,sum(refund_amount_usd) as total_refund_amount
    from refunds
    group by 1
)

,credit_amounts as (
    select
        order_id
        ,max(credit_type = 'FREE_SHIPPING') as has_free_shipping
        ,sum(discount_percent) as discount_percent
        ,sum(credit_discount_usd) as credit_discount_usd
    from credits
    group by 1
)

,order_joins as (
    select
        orders.order_id
        ,orders.parent_order_id
        ,orders.order_token
        ,orders.user_id
        ,orders.subscription_id
        ,orders.fc_id
        ,orders.visit_id
        ,orders.order_type
        ,orders.stripe_failure_code
        ,orders.order_delivery_street_address_1
        ,orders.order_delivery_street_address_2
        ,orders.order_delivery_city
        ,orders.order_delivery_state
        ,orders.order_delivery_postal_code
        ,orders.billing_address_1
        ,orders.billing_address_2
        ,orders.billing_city
        ,orders.billing_state
        ,orders.billing_postal_code
        ,orders.order_total_price_usd
        ,zeroifnull(coalesce(order_statements.product_revenue_usd,bid_amounts.bid_amount)) as product_revenue_usd
        ,zeroifnull(coalesce(order_statements.freight_revenue_usd,orders.order_shipping_fee_usd)) as shipping_revenue_usd
        ,orders.order_total_discount_usd
    
        ,case
            when credit_amounts.order_id is null then FALSE 
            else credit_amounts.has_free_shipping
         end as has_free_shipping
    
        ,zeroifnull(credit_amounts.discount_percent) as discount_percent
        ,zeroifnull(credit_discount_usd) as credit_discount_usd
        ,zeroifnull(refund_amounts.total_refund_amount) as refund_amount_usd
        ,flags.is_ala_carte_order
        ,flags.is_membership_order
        ,flags.is_completed_order
        ,flags.is_paid_order
        ,flags.is_cancelled_order
        ,flags.is_abandonded_order
        ,flags.is_gift_order
        ,flags.is_bulk_gift_order
        ,flags.is_gift_card_order
        ,ranks.overall_order_rank
        ,ranks.completed_order_rank
        ,ranks.paid_order_rank
        ,ranks.cancelled_order_rank
        ,ranks.membership_order_rank
        ,ranks.ala_carte_order_rank
        ,ranks.paid_membership_order_rank
        ,ranks.paid_ala_carte_order_rank
        ,orders.order_created_at_utc
        ,orders.order_updated_at_utc
        ,orders.order_checkout_completed_at_utc
        ,orders.order_cancelled_at_utc
        ,orders.order_paid_at_utc
        ,orders.order_first_stuck_at_utc
        ,orders.order_scheduled_fulfillment_date_utc
        ,orders.order_scheduled_arrival_date_utc
    from orders
        left join refund_amounts on orders.order_id = refund_amounts.order_id
        left join credit_amounts on orders.order_id = credit_amounts.order_id
        left join order_statements on orders.order_id = order_statements.order_id
        left join bid_amounts on orders.order_id = bid_amounts.order_id
        left join flags on orders.order_id = flags.order_id
        left join ranks on orders.order_id = ranks.order_id
)

,add_discount_amounts as (
    select
        order_id
        ,parent_order_id
        ,order_token
        ,user_id
        ,subscription_id
        ,fc_id
        ,visit_id
        ,order_type
        ,stripe_failure_code
        ,order_delivery_street_address_1
        ,order_delivery_street_address_2
        ,order_delivery_city
        ,order_delivery_state
        ,order_delivery_postal_code
        ,billing_address_1
        ,billing_address_2
        ,billing_city
        ,billing_state
        ,billing_postal_code
        ,order_total_price_usd
        ,product_revenue_usd
        ,shipping_revenue_usd
        ,order_total_discount_usd
    
        ,case
            when has_free_shipping then (product_revenue_usd * discount_percent) + shipping_revenue_usd + credit_discount_usd
            else (product_revenue_usd * discount_percent) + credit_discount_usd
         end as discounts_usd
    
        ,has_free_shipping
        ,discount_percent
        ,credit_discount_usd
        ,refund_amount_usd
        ,is_ala_carte_order
        ,is_membership_order
        ,is_completed_order
        ,is_paid_order
        ,is_cancelled_order
        ,is_abandonded_order
        ,is_gift_order
        ,is_bulk_gift_order
        ,is_gift_card_order
        ,overall_order_rank
        ,completed_order_rank
        ,paid_order_rank
        ,cancelled_order_rank
        ,membership_order_rank
        ,ala_carte_order_rank
        ,paid_membership_order_rank
        ,paid_ala_carte_order_rank
        ,order_created_at_utc
        ,order_updated_at_utc
        ,order_checkout_completed_at_utc
        ,order_cancelled_at_utc
        ,order_paid_at_utc
        ,order_first_stuck_at_utc
        ,order_scheduled_fulfillment_date_utc
        ,order_scheduled_arrival_date_utc
    from order_joins
)

,final as (
    select
        order_id
        ,parent_order_id
        ,order_token
        ,user_id
        ,subscription_id
        ,fc_id
        ,visit_id
        ,order_type
        ,stripe_failure_code
        ,order_delivery_street_address_1
        ,order_delivery_street_address_2
        ,order_delivery_city
        ,order_delivery_state
        ,order_delivery_postal_code
        ,billing_address_1
        ,billing_address_2
        ,billing_city
        ,billing_state
        ,billing_postal_code
        ,order_total_price_usd
        ,product_revenue_usd
        ,shipping_revenue_usd
        ,order_total_discount_usd
        ,discounts_usd
        ,has_free_shipping
        ,discount_percent
        ,credit_discount_usd
        ,refund_amount_usd
        ,product_revenue_usd + shipping_revenue_usd as gross_revenue
        ,product_revenue_usd + shipping_revenue_usd - discounts_usd - refund_amount_usd as net_revenue
        ,is_ala_carte_order
        ,is_membership_order
        ,is_completed_order
        ,is_paid_order
        ,is_cancelled_order
        ,is_abandonded_order
        ,is_gift_order
        ,is_bulk_gift_order
        ,is_gift_card_order
        ,overall_order_rank
        ,completed_order_rank
        ,paid_order_rank
        ,cancelled_order_rank
        ,membership_order_rank
        ,ala_carte_order_rank
        ,paid_membership_order_rank
        ,paid_ala_carte_order_rank
        ,order_created_at_utc
        ,order_updated_at_utc
        ,order_checkout_completed_at_utc
        ,order_cancelled_at_utc
        ,order_paid_at_utc
        ,order_first_stuck_at_utc
        ,order_scheduled_fulfillment_date_utc
        ,order_scheduled_arrival_date_utc
    from add_discount_amounts
)

select * from final

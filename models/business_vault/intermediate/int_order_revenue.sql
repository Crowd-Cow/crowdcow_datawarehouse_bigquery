with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,credit as ( select * from {{ ref('credits') }} )
,refund as ( select * from {{ ref('stg_cc__refunds') }} )

,bid_amounts as (
    select 
        order_id
        ,sum(order_item_revenue) as product_revenue_usd
        ,sum(order_item_discount) as order_item_discount_usd
    from order_item
    group by 1
)

,credit_amounts as (
    select
        order_id
        ,sum(discount_percent) as discount_percent
        ,sum(credit_discount_usd) as discount_amount_usd
        
        /**** Breakdown the total credit for an order into various credit categories for financial reporting in Looker ****/
        ,count_if(credit_type = 'FREE_SHIPPING') as free_shipping_credit_count
        ,zeroifnull(sum(case when credit_type = 'FREE_SHIPPING' then credit_discount_usd end)) as free_shipping_credit
        ,zeroifnull(sum())

    from credit
    group by 1
)

,refund_amounts as (
    select
        stripe_charge_id
        ,sum(refund_amount_usd) as refund_amount_usd
    from refund
    group by 1
)

,revenue_joins as (
    select
        orders.order_id
        ,orders.parent_order_id
        ,orders.order_shipping_fee_usd
        ,zeroifnull(bid_amounts.product_revenue_usd) as product_revenue_usd
        ,zeroifnull(bid_amounts.order_item_discount_usd) as order_item_discount_usd
        ,zeroifnull(credit_amounts.discount_amount_usd) as discount_amount_usd
        ,zeroifnull(refund_amounts.refund_amount_usd) as refund_amount_usd
        ,zeroifnull(credit_amounts.discount_percent) as discount_percent
        ,zeroifnull(credit_amounts.free_shipping_credit_count) as free_shipping_credit_count
        ,zeroifnull(credit_amounts.free_shipping_credit) as free_shipping_credit
    from orders
        left join bid_amounts on orders.order_id = bid_amounts.order_id
        left join credit_amounts on orders.order_id = credit_amounts.order_id
        left join refund_amounts on orders.stripe_charge_id = refund_amounts.stripe_charge_id
)

,fix_shipping_credits as (
    /*** Some records in the source `cc.credits` table still have a 0 value for the discount amount. ***/
    /*** If the order has a free shipping credit, but a 0 value for the discount amount, we take the shipping fee for the order as the shipping credit amount ***/

    select
        order_id
        ,parent_order_id
        ,order_shipping_fee_usd
        ,product_revenue_usd + order_shipping_fee_usd as gross_revenue_usd
        ,product_revenue_usd
        ,discount_percent
        ,refund_amount_usd

        ,case
            when free_shipping_credit_count > 0 and free_shipping_credit = 0 then discount_amount_usd + order_item_discount_usd + order_shipping_fee_usd
            else discount_amount_usd + order_item_discount_usd
         end as total_order_discount

        ,case
            when free_shipping_credit_count > 0 and free_shipping_credit = 0 then discount_amount_usd + order_shipping_fee_usd
            else discount_amount_usd
         end as order_discount_no_item_discount_amount

        ,order_item_discount_usd

        ,case
            when free_shipping_credit_count > 0 and free_shipping_credit = 0 then order_shipping_fee_usd
            else free_shipping_credit
         end as free_shipping_credit
    from revenue_joins
)

,revenue_calculations as (
    select
        order_id
        ,order_shipping_fee_usd
        ,product_revenue_usd + order_shipping_fee_usd as gross_revenue_usd
        ,product_revenue_usd
        ,total_order_discount
        ,order_discount_no_item_discount_amount
        ,order_item_discount_usd
        ,discount_percent
        ,refund_amount_usd

        ,case 
            when parent_order_id is not null then product_revenue_usd
            else product_revenue_usd + order_shipping_fee_usd - (order_discount_no_item_discount_amount + order_item_discount_usd) - refund_amount_usd 
         end as net_revenue_usd

        ,free_shipping_credit
        ,cs_cow_cash_credit
        ,redeemed_gift_card_credit
        ,new_member_promotion_credit
        ,replacement_item_credit
        ,marketing_pr_credit
        ,cow_cash_promotion_credit
        ,cow_cash_gift_card_promotion_credit
        ,subscriber_five_pct_credit
        ,corp_gift_credit
        ,sales_marketing_referral_credit
        ,new_customer_referral_credit
        ,customer_retention_credit
        ,inr_credit
        ,price_match_credit
    from fix_shipping_credits
)

select * from revenue_calculations

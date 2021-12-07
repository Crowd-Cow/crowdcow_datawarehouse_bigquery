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
        ,zeroifnull(sum(case when credit_type = 'FREE_SHIPPING' then credit_discount_usd end)) as free_shipping_credits
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'CUSTOMER_SERVICE' and awarded_cow_cash_message not like '%GIVEAWAY%' then credit_discount_usd end)) as cs_cow_cash_credits
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'GIFT_CARD' then credit_discount_usd end)) as redeemed_gift_card_credit
        
        ,zeroifnull(
            sum(
                case when credit_description like any ('%REPLACEMENT%','%MISSING%','%LOST%','%STRIPE%','%ALREADY PAID%'
                                    ,'%PAID FOR%','%MANUAL%','%PREPAYMENT%','%PRE_PAYMENT%','%PREORDER%'
                                    ,'%PRE_ORDER%','%PRESALE%','%PRE_SALE%') 
                then credit_discount_usd end
            )
        ) as replacement_item_credits
        
        ,zeroifnull(
            sum(
                case when credit_description like any ('%EVENT%','%SAMPLE%','%TASTING%','%INFLUENCER%','%GIVEAWAY%','%GIFT%','%MARKETING%')
                    or awarded_cow_cash_message like '%GIVEAWAY%'
                then credit_discount_usd end
            )
        ) as marketing_pr_credits
        
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'PROMOTION' or awarded_cow_cash_message like '%POSTCARD%' then credit_discount_usd end)) as other_credits
        ,zeroifnull(sum(case when credit_type = 'SUBSCRIPTION_FIVE_PERCENT' then credit_discount_usd end)) as subscriber_five_pct_credits
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'BULK_ORDER' then credit_discount_usd end)) as corp_gifts
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'REFERRAL' then credit_discount_usd end)) as sales_marketing_referral
        ,zeroifnull(sum(case when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' then credit_discount_usd end)) as new_customer_referral

    from credit
    group by 1
)

,refund_amounts as (
    select
        order_id
        ,sum(refund_amount_usd) as refund_amount_usd
    from refund
    group by 1
)

,revenue_joins as (
    select
        orders.order_id
        ,orders.order_shipping_fee_usd
        ,zeroifnull(bid_amounts.product_revenue_usd) as product_revenue_usd
        ,zeroifnull(bid_amounts.order_item_discount_usd) as order_item_discount_usd
        ,zeroifnull(credit_amounts.discount_amount_usd) as discount_amount_usd
        ,zeroifnull(refund_amounts.refund_amount_usd) as refund_amount_usd
        ,zeroifnull(credit_amounts.discount_percent) as discount_percent
    from orders
        left join bid_amounts on orders.order_id = bid_amounts.order_id
        left join credit_amounts on orders.order_id = credit_amounts.order_id
        left join refund_amounts on orders.order_id = refund_amounts.order_id
)

,revenue_calculations as (
    select
        order_id
        ,order_shipping_fee_usd
        ,product_revenue_usd + order_shipping_fee_usd as gross_revenue_usd
        ,product_revenue_usd
        ,discount_amount_usd + order_item_discount_usd as discount_amount_usd
        ,discount_percent
        ,refund_amount_usd
        ,product_revenue_usd + order_shipping_fee_usd - (discount_amount_usd + order_item_discount_usd) - refund_amount_usd as net_revenue_usd
    from revenue_joins
)

select * from revenue_calculations

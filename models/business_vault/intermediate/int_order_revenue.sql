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
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'CUSTOMER_SERVICE' and awarded_cow_cash_message not like '%GIVEAWAY%' then credit_discount_usd end)) as cs_cow_cash_credit
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'GIFT_CARD' then credit_discount_usd end)) as redeemed_gift_card_credit
        ,zeroifnull(sum(case when is_new_member_promotion then credit_discount_usd end)) as new_member_promotion_credit
        
        ,zeroifnull(
            sum(
                case when credit_description like any ('%REPLACE%','%MISSING%','%LOST%','%STRIPE%','%ALREADY PAID%'
                                    ,'%PAID FOR%','%MANUAL%','%PREPAYMENT%','%PRE_PAYMENT%','%PREORDER%','%QUALITY%','%ARRIVE%'
                                    ,'%PRE_ORDER%','%PRESALE%','%PRE_SALE%','%LATE%','%THAW%','%RECEIVED%','%FAIL%','%DELAY%'
                                    ,'%SHIPPED%','%NOT%SHIP%','%WRONG%','%DAMAGE%','%ACCURACY%') 
                                    and credit_description not like '%WHOLESALE%'
                then credit_discount_usd end
            )
        ) as replacement_item_credit
        
        ,zeroifnull(
            sum(
                case when (credit_description like any ('%EVENT%','%SAMPLE%','%TASTING%','%INFLUENCER%','%GIVEAWAY%','%GIFT%','%MARKETING%')
                    or awarded_cow_cash_message like '%GIVEAWAY%') and not(credit_description like any ('%REPLACE%','%THAW%'))
                then credit_discount_usd end
            )
        ) as marketing_pr_credit
        
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'PROMOTION' or awarded_cow_cash_message like '%POSTCARD%' then credit_discount_usd end)) as cow_cash_promotion_credit
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'GIFT_CARD_PROMOTION' then credit_discount_usd end)) as cow_cash_gift_card_promotion_credit
        ,zeroifnull(sum(case when credit_type = 'SUBSCRIPTION_FIVE_PERCENT' then credit_discount_usd end)) as subscriber_five_pct_credit
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type = 'BULK_ORDER' then credit_discount_usd end)) as corp_gift_credit
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type like '%REFER%' then credit_discount_usd end)) as sales_marketing_referral_credit
        ,zeroifnull(sum(case when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' or promotion_type = 'GIFT_CODE_PROMOTION' then credit_discount_usd end)) as new_customer_referral_credit
        ,zeroifnull(sum(case when awarded_cow_cash_entry_type like '%RETENTION%' then credit_discount_usd end)) as customer_retention_credit
        ,zeroifnull(sum(case when credit_description like '%INR%' then credit_discount_usd end)) as inr_credit
        ,zeroifnull(sum(case when credit_type = 'DOLLAR_AMOUNT' and credit_description like '%PRICE%' then credit_discount_usd end)) as price_match_credit

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
        ,zeroifnull(credit_amounts.cs_cow_cash_credit) as cs_cow_cash_credit
        ,zeroifnull(credit_amounts.redeemed_gift_card_credit) as redeemed_gift_card_credit
        ,zeroifnull(credit_amounts.new_member_promotion_credit) as new_member_promotion_credit
        ,zeroifnull(credit_amounts.replacement_item_credit) as replacement_item_credit
        ,zeroifnull(credit_amounts.marketing_pr_credit) as marketing_pr_credit
        ,zeroifnull(credit_amounts.cow_cash_promotion_credit) as cow_cash_promotion_credit
        ,zeroifnull(credit_amounts.cow_cash_gift_card_promotion_credit) as cow_cash_gift_card_promotion_credit
        ,zeroifnull(credit_amounts.subscriber_five_pct_credit) as subscriber_five_pct_credit
        ,zeroifnull(credit_amounts.corp_gift_credit) as corp_gift_credit
        ,zeroifnull(credit_amounts.sales_marketing_referral_credit) as sales_marketing_referral_credit
        ,zeroifnull(credit_amounts.new_customer_referral_credit) as new_customer_referral_credit
        ,zeroifnull(credit_amounts.customer_retention_credit) as customer_retention_credit
        ,zeroifnull(credit_amounts.inr_credit) as inr_credit
        ,zeroifnull(credit_amounts.price_match_credit) as price_match_credit
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

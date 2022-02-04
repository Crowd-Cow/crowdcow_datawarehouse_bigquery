with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,discount as ( select * from {{ ref('discounts') }} )
,refund as ( select * from {{ ref('stg_stripe__refunds') }} )

,bid_amounts as (
    select 
        order_id
        ,sum(bid_gross_product_revenue) as gross_product_revenue
    from order_item
    group by 1
)

,discount_amounts as (
    select
        order_id
        ,sum(discount_usd) as total_discount_amount_usd
        
        /**** Breakdown the total credit for an order into various credit categories for financial reporting in Looker ****/
        ,count_if(business_group = 'FREE SHIPPING') as free_shipping_credit_count
        ,sum(case when revenue_waterfall_bucket = 'FREE SHIPPING DISCOUNT' then discount_usd end) as free_shipping_discount
        ,sum(case when revenue_waterfall_bucket = 'MEMBERSHIP DISCOUNT' then discount_usd end) as membership_discount
        ,sum(case when revenue_waterfall_bucket = 'MERCH DISCOUNT' then discount_usd end) as merch_discount
        ,sum(case when revenue_waterfall_bucket = 'FREE PROTEIN PROMOTION' then discount_usd end) as free_protein_promotion
        ,sum(case when revenue_waterfall_bucket = 'NEW MEMBER DISCOUNT' then discount_usd end) as new_member_discount
        ,sum(case when revenue_waterfall_bucket = 'GIFT REDEMPTION' then discount_usd end) as gift_redemption
        ,sum(case when revenue_waterfall_bucket = 'OTHER DISCOUNT' then discount_usd end) as other_discount

    from discount
    group by 1
)

,refund_amounts as (
    select
        stripe_charge_id
        ,sum(refund_amount_usd) as refund_amount_usd
    from refund
    group by 1
)

,revenue_component_joins as (
    select
        orders.order_id
        ,orders.order_type
        ,parent_order_id
        ,orders.order_shipping_fee_usd as shipping_revenue
        ,zeroifnull(bid_amounts.gross_product_revenue) as gross_product_revenue
        ,zeroifnull(discount_amounts.free_shipping_credit_count) as free_shipping_credit_count
        ,zeroifnull(discount_amounts.free_shipping_discount) * -1 as free_shipping_discount
        ,zeroifnull(discount_amounts.membership_discount) * -1 as membership_discount
        ,zeroifnull(discount_amounts.merch_discount) * -1 as merch_discount
        ,zeroifnull(discount_amounts.free_protein_promotion) * -1 as free_protein_promotion
        ,zeroifnull(discount_amounts.new_member_discount) * -1 as new_member_discount
        ,zeroifnull(discount_amounts.gift_redemption) * -1 as gift_redemption
        ,zeroifnull(discount_amounts.other_discount) * -1 as other_discount
        ,zeroifnull(refund_amounts.refund_amount_usd) * -1 as refund_amount
    from orders
        left join bid_amounts on orders.order_id = bid_amounts.order_id
        left join discount_amounts on orders.order_id = discount_amounts.order_id
        left join refund_amounts on orders.stripe_charge_id = refund_amounts.stripe_charge_id
)

,fix_shipping_credits as (
    /*** Some records in the source `cc.credits` table still have a 0 value for the discount amount. ***/
    /*** If the order has a free shipping credit, but a 0 value for the discount amount, we take the shipping fee for the order as the shipping credit amount ***/

    select
        order_id
        ,parent_order_id
        ,order_type
        ,shipping_revenue
        ,gross_product_revenue

        ,case
            when free_shipping_credit_count > 0 and free_shipping_discount = 0 then shipping_revenue * -1
            else free_shipping_discount
         end as free_shipping_discount

        ,membership_discount
        ,merch_discount
        ,free_protein_promotion
        ,new_member_discount
        ,gift_redemption
        ,other_discount
        ,refund_amount

    from revenue_component_joins
)

,revenue_calculations as (
    select
        order_id
        ,gross_product_revenue
        ,membership_discount
        ,merch_discount
        ,free_protein_promotion
        
        ,gross_product_revenue 
         + membership_discount 
         + merch_discount
         + free_protein_promotion as net_product_revenue
        
        ,shipping_revenue
        ,free_shipping_discount
        
        ,gross_product_revenue 
         + membership_discount 
         + merch_discount
         + free_protein_promotion
         + shipping_revenue 
         + free_shipping_discount as gross_revenue
        
        ,new_member_discount
        ,gift_redemption
        ,refund_amount
        ,other_discount

        ,round(
            gross_product_revenue 
            + membership_discount 
            + merch_discount
            + free_protein_promotion
            + shipping_revenue 
            + free_shipping_discount
            + new_member_discount
            + refund_amount
            + gift_redemption
            + other_discount
        ,2) as net_revenue

    from fix_shipping_credits
)

select * from revenue_calculations

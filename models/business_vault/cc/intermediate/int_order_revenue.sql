with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,discount as ( select * from {{ ref('discounts') }} )
,refund as ( select * from {{ ref('int_refunds') }} )

,bid_amounts as (
    select 
        order_id
        ,sum(bid_gross_product_revenue) as gross_product_revenue
        ,sum(bid_price_paid_usd) as bid_price_paid_usd
    from order_item
    group by 1
)

,discount_amounts as (
    select
        order_id
        ,sum(discount_usd) as total_discount_amount_usd
        
        /**** Breakdown the total credit for an order into various credit categories for financial reporting in Looker ****/
        ,countif(business_group = 'FREE SHIPPING') as free_shipping_credit_count
        ,sum(case when revenue_waterfall_bucket = 'FREE SHIPPING DISCOUNT' then discount_usd end) as free_shipping_discount
        ,sum(case when revenue_waterfall_bucket = 'MEMBERSHIP DISCOUNT' then discount_usd end) as membership_discount
        ,sum(case when revenue_waterfall_bucket = 'MERCH DISCOUNT' then discount_usd end) as merch_discount
        ,sum(case when revenue_waterfall_bucket = 'FREE PROTEIN PROMOTION' then discount_usd end) as free_protein_promotion
        ,sum(case when revenue_waterfall_bucket = 'OTHER ITEM LEVEL PROMOTIONS' then discount_usd end) as item_promotion
        ,sum(case when revenue_waterfall_bucket = 'NEW MEMBER DISCOUNT' then discount_usd end) as new_member_discount
        ,sum(case when revenue_waterfall_bucket = 'GIFT REDEMPTION' then discount_usd end) as gift_redemption
        ,sum(case when revenue_waterfall_bucket = 'OTHER DISCOUNT' then discount_usd end) as other_discount
        ,sum(case when revenue_waterfall_bucket = 'MOOLAH ITEM DISCOUNT' then discount_usd end) as moolah_item_discount
        ,sum(case when revenue_waterfall_bucket = 'MOOLAH ORDER DISCOUNT' then discount_usd end) as moolah_order_discount

    from discount
    group by 1
)

,revenue_component_joins as (
    select
        orders.order_id
        ,orders.order_type
        ,parent_order_id
        ,coalesce(orders.order_shipping_fee_usd,0) + coalesce(order_expedited_shipping_fee_usd,0) as shipping_revenue
        ,coalesce(bid_amounts.gross_product_revenue, 0) as gross_product_revenue
        ,coalesce(bid_amounts.bid_price_paid_usd, 0) as bid_price_paid_usd
        ,coalesce(discount_amounts.free_shipping_credit_count, 0) as free_shipping_credit_count
        ,coalesce(discount_amounts.free_shipping_discount, 0) * -1 as free_shipping_discount
        ,coalesce(discount_amounts.membership_discount, 0) * -1 as membership_discount
        ,coalesce(discount_amounts.merch_discount, 0) * -1 as merch_discount
        ,coalesce(discount_amounts.free_protein_promotion, 0) * -1 as free_protein_promotion
        ,coalesce(discount_amounts.item_promotion, 0) * -1 as item_promotion
        ,coalesce(discount_amounts.moolah_item_discount, 0)*-1 as moolah_item_discount
        ,coalesce(discount_amounts.moolah_order_discount, 0)*-1 as moolah_order_discount
        ,coalesce(discount_amounts.new_member_discount, 0) * -1 as new_member_discount
        ,coalesce(discount_amounts.gift_redemption, 0) * -1 as gift_redemption
        ,coalesce(discount_amounts.other_discount, 0) * -1 as other_discount
        ,coalesce(refund.refund_amount_usd, 0) * -1 as refund_amount
    from orders
        left join bid_amounts on orders.order_id = bid_amounts.order_id
        left join discount_amounts on orders.order_id = discount_amounts.order_id
        left join refund on orders.stripe_charge_id = refund.stripe_charge_id
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
        ,bid_price_paid_usd

        ,case
            when free_shipping_credit_count > 0 and free_shipping_discount = 0 then shipping_revenue * -1
            else free_shipping_discount
         end as free_shipping_discount

        ,membership_discount
        ,merch_discount
        ,free_protein_promotion
        ,item_promotion
        ,moolah_item_discount
		,moolah_order_discount
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
        ,bid_price_paid_usd
        ,membership_discount
        ,merch_discount
        ,free_protein_promotion
        ,item_promotion
        ,moolah_item_discount
        
        ,gross_product_revenue 
         + membership_discount 
         + merch_discount
         + free_protein_promotion
         + item_promotion 
         + moolah_item_discount as net_product_revenue
        
        ,shipping_revenue
        ,free_shipping_discount
        
        ,gross_product_revenue 
         + membership_discount 
         + merch_discount
         + free_protein_promotion
         + item_promotion
         + moolah_item_discount
         + shipping_revenue 
         + free_shipping_discount as gross_revenue
        
        ,new_member_discount
        ,gift_redemption
        ,refund_amount
        ,moolah_order_discount
        ,other_discount

        ,round(
            gross_product_revenue 
            + membership_discount 
            + merch_discount
            + free_protein_promotion
            + item_promotion
            + moolah_item_discount
            + shipping_revenue 
            + free_shipping_discount
            + moolah_order_discount
            + new_member_discount
            + refund_amount
            + gift_redemption
            + other_discount
        ,2) as net_revenue

    from fix_shipping_credits
)

select * from revenue_calculations

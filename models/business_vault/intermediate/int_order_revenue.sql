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
        ,orders.order_shipping_fee_usd
        ,zeroifnull(bid_amounts.gross_product_revenue) as gross_product_revenue
        ,zeroifnull(discount_amounts.free_shipping_credit_count) as free_shipping_credit_count
        ,zeroifnull(discount_amounts.free_shipping_discount) * -1 as free_shipping_discount
        ,zeroifnull(discount_amounts.membership_discount) * -1 as membership_discount
        ,zeroifnull(discount_amounts.merch_discount) * -1 as merch_discount
        ,zeroifnull(discount_amounts.free_protein_promotion) * -1 as free_protein_promotion
        ,zeroifnull(discount_amounts.new_member_discount) * -1 as new_member_discount
        ,zeroifnull(discount_amounts.gift_redemption) * -1 as gift_redemption
        ,zeroifnull(discount_amounts.other_discount) * -1 as other_discount
        ,zeroifnull(refund_amounts.refund_amount_usd) * -1 as refund_amount_usd
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
        ,order_shipping_fee_usd
        ,gross_product_revenue

        ,case
            when free_shipping_credit_count > 0 and free_shipping_discount = 0 then order_shipping_fee_usd * -1
            else free_shipping_discount
         end as free_shipping_discount

        ,membership_discount
        ,merch_discount
        ,free_protein_promotion
        ,new_member_discount
        ,gift_redemption
        ,other_discount
        ,refund_amount_usd

    from revenue_component_joins
)

,create_bulk_order_revenue as (
    select 
        parent_order_id
        ,count(order_id) as child_order_count
        ,sum(gross_product_revenue) as gross_product_revenue
        ,sum(membership_discount) as membership_discount
        ,sum(merch_discount) as merch_discount
        ,sum(free_protein_promotion) as free_protein_promotion
        ,sum(order_shipping_fee_usd) as shipping_revenue
        ,sum(free_shipping_discount) as free_shipping_discount
        ,sum(new_member_discount) as new_member_discount
        ,sum(refund_amount_usd) as refund_amount
        ,sum(0) as gift_redemption --gift redemption value for bulk parent order is set to $0 since there shouldn't be any gift redemptions associated with parent orders
        ,sum(other_discount) as other_discount
    from fix_shipping_credits
    where parent_order_id is not null
    group by 1
)

,replace_bulk_parent_order_revenue as (
    select
        fix_shipping_credits.order_id
        ,fix_shipping_credits.order_type
        ,fix_shipping_credits.parent_order_id
        ,create_bulk_order_revenue.parent_order_id is not null as has_children

        /**** If the order type is 'BULK ORDER' and has no matching children orders all revenue components should be defaulted to $0 ****/
        /**** These are parent orders with no children and according to eng should be treated as bugs and ignored ****/

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.gross_product_revenue, fix_shipping_credits.gross_product_revenue)
         end  as gross_product_revenue

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.membership_discount, fix_shipping_credits.membership_discount)
         end as membership_discount

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.merch_discount, fix_shipping_credits.merch_discount) 
         end as merch_discount

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.free_protein_promotion, fix_shipping_credits.free_protein_promotion) 
         end as free_protein_promotion

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.shipping_revenue, fix_shipping_credits.order_shipping_fee_usd) 
         end as shipping_revenue

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.free_shipping_discount, fix_shipping_credits.free_shipping_discount) 
         end as free_shipping_discount

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.new_member_discount, fix_shipping_credits.new_member_discount) 
         end as new_member_discount

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.refund_amount, fix_shipping_credits.refund_amount_usd) 
         end as refund_amount

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.gift_redemption, fix_shipping_credits.gift_redemption) 
        end as gift_redemption

        ,case
            when fix_shipping_credits.order_type = 'BULK ORDER' and create_bulk_order_revenue.parent_order_id is null then 0
            else coalesce(create_bulk_order_revenue.other_discount, fix_shipping_credits.other_discount) 
         end as other_discount

    from fix_shipping_credits
        left join create_bulk_order_revenue on fix_shipping_credits.order_id = create_bulk_order_revenue.parent_order_id
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

    from replace_bulk_parent_order_revenue
)

select * from revenue_calculations

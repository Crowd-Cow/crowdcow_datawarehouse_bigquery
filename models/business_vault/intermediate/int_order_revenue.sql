with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,discount as ( select * from {{ ref('discounts') }} )
,refund as ( select * from {{ ref('stg_cc__refunds') }} )

,bid_amounts as (
    select 
        order_id
        ,sum(order_item_revenue) as gross_product_revenue
    from order_item
    group by 1
)

,discount_amounts as (
    select
        order_id
        ,sum(discount_usd) as total_discount_amount_usd
        
        /**** Breakdown the total credit for an order into various credit categories for financial reporting in Looker ****/
        ,count_if(business_group = 'FREE SHIPPING') as free_shipping_credit_count
        ,zeroifnull(sum(case when business_group = 'FREE SHIPPING' then discount_usd end)) as free_shipping_discount
        ,zeroifnull(sum(case when business_group = 'MEMBERSHIP 5%' then discount_usd end)) as membership_discount
        ,zeroifnull(sum(case when business_group = 'MERCHANDISING DISCOUNT' then discount_usd end)) as merch_discount
        ,zeroifnull(
            sum(
                case 
                    when business_group in ('ACQUISITION MARKETING - PROMOTION CREDITS','MEMBERSHIP PROMOTIONS','OTHER ITEM LEVEL PROMOTIONS') 
                        and is_new_member_promotion 
                    then discount_usd 
                end
            )
        ) as new_member_discount
        ,zeroifnull(
            sum(
                case
                    when business_group in ('ACQUISITION MARKETING - GIFT', 'ACQUISITION MARKETING - INFLUENCER','ACQUISITION MARKETING - MEMBER REFERRAL'
                        ,'ACQUISITION MARKETING - PROMOTION CREDITS','CARE CREDITS','OTHER - UNKNOWN','OTHER ITEM LEVEL PROMOTIONS','RETENTION MARKETING')
                        and not is_new_member_promotion
                    then discount_usd
                end
            )
        ) as other_discount

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

,revenue_joins as (
    select
        orders.order_id
        ,orders.parent_order_id
        ,orders.order_shipping_fee_usd
        ,zeroifnull(bid_amounts.gross_product_revenue) as gross_product_revenue
        ,zeroifnull(discount_amounts.free_shipping_credit_count) as free_shipping_credit_count
        ,zeroifnull(discount_amounts.free_shipping_discount) as free_shipping_discount
        ,zeroifnull(discount_amounts.membership_discount) as membership_discount
        ,zeroifnull(discount_amounts.merch_discount) as merch_discount
        ,zeroifnull(discount_amounts.new_member_discount) as new_member_discount
        ,zeroifnull(discount_amounts.other_discount) as other_discount
        ,zeroifnull(refund_amounts.refund_amount_usd) as refund_amount_usd
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
        ,order_shipping_fee_usd
        ,gross_product_revenue

        ,case
            when free_shipping_credit_count > 0 and free_shipping_discount = 0 then order_shipping_fee_usd
            else free_shipping_discount
         end as free_shipping_discount

         ,membership_discount
         ,merch_discount
         ,new_member_discount
         ,other_discount
        ,refund_amount_usd

    from revenue_joins
)

,revenue_calculations as (
    select
        order_id
        ,gross_product_revenue
        ,membership_discount
        ,merch_discount
        
        ,gross_product_revenue 
         - membership_discount 
         - merch_discount as net_product_revenue
        
        ,order_shipping_fee_usd
        ,free_shipping_discount
        
        ,gross_product_revenue 
         - membership_discount 
         - merch_discount
         + order_shipping_fee_usd 
         - free_shipping_discount as gross_revenue
        
        ,new_member_discount
        ,refund_amount_usd
        ,other_discount

        ,round(
            case 
                when parent_order_id is not null then 
                    gross_product_revenue 
                    - membership_discount 
                    - merch_discount
                else 
                    gross_product_revenue 
                    - membership_discount 
                    - merch_discount
                    + order_shipping_fee_usd 
                    - free_shipping_discount
                    - new_member_discount
                    - refund_amount_usd
                    - other_discount
            end
        ,2) as net_revenue

    from fix_shipping_credits
)

select * from revenue_calculations

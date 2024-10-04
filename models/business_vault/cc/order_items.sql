{{
    config(
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

bids as ( select * from {{ ref('stg_cc__bids') }} )
,bid_items as (select * from {{ ref('stg_cc__bid_items') }} )

-- The system allows for multiple item level credit for the same item which causes duplicate order items when joining to credits ***/
-- This doesn't allow us to accurately show which promos were applied to which item since the rest of the financial info (revenue, discounts, costs, etc) 
-- are at the item level and not the item/credit level. For now, we are taking the total amount of item credits for an item and the highest value credit as the one that was applied to the item ***/
,item_credits as ( 
    select distinct
        bid_id
        ,first_value(promotion_id) over(partition by bid_id order by credit_discount_usd desc) as promotion_id
        ,first_value(promotion_source) over(partition by bid_id order by credit_discount_usd desc) as promotion_source
        ,sum(credit_discount_usd) over(partition by bid_id) as credit_discount_usd
    from {{ ref('stg_cc__credits') }}
    where bid_id is not null
)

,order_item_joins as (
    select
        bids.order_id
        ,bids.bid_id
        ,bids.bid_item_id

        ,if(
            item_credits.promotion_id is not null
            ,item_credits.promotion_id
            ,bids.promotion_id
        ) as promotion_id

        ,case
            when item_credits.promotion_id is not null then item_credits.promotion_source
            when item_credits.promotion_id is null and bids.promotion_id is not null and bids.promotion_source is not null then bids.promotion_source
            when item_credits.promotion_id is null and bids.promotion_id is not null then 'PROMOTION'
            else null
        end as promotion_source
        
        ,bids.bid_token
        ,bids.product_id
        ,bids.product_name
        ,bid_items.bid_item_name
        ,bid_items.bid_item_type
        ,bid_items.bid_item_subtype
        ,coalesce(bid_items.bid_item_list_price_usd,bid_items.bid_item_price_usd) as bid_item_list_price_usd
        ,coalesce(bid_items.bid_item_member_price_usd,bid_items.bid_item_price_usd) as bid_item_member_price_usd
        ,coalesce(bid_items.bid_item_non_member_price_usd, bid_items.bid_item_price_usd) as bid_item_non_member_price_usd
        ,bids.autofill_reason
        ,bids.fill_type
        ,coalesce(bids.bid_list_price_usd,bids.item_price_usd) as bid_list_price_usd
        ,coalesce(bids.item_price_usd, bids.bid_price_paid_usd) as bid_price_paid_usd
        ,coalesce(bids.bid_non_member_price_usd,bids.item_price_usd) as bid_non_member_price_usd
        ,coalesce(bids.bid_member_price_usd,bids.item_price_usd) as bid_member_price_usd
        ,coalesce(item_credits.credit_discount_usd,0) as bid_item_credit_usd
        ,bids.is_fulfillment_at_risk
        ,bids.bid_quantity
        ,bids.created_at_utc
        ,bids.updated_at_utc
        ,bids.first_stuck_at_utc
        ,bids.used_member_pricing
    from bids
        left join item_credits on bids.bid_id = item_credits.bid_id
        left join bid_items on bids.bid_item_id = bid_items.bid_item_id
            and bids.created_at_utc >= bid_items.adjusted_dbt_valid_from
            and bids.created_at_utc < bid_items.adjusted_dbt_valid_to
)

,update_promotion_bid_prices as (
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,promotion_id
        ,promotion_source
        ,bid_token
        ,product_id
        ,product_name
        ,bid_item_name
        ,bid_item_type
        ,bid_item_subtype
        ,autofill_reason
        ,fill_type
        
        ,case
            when promotion_id is not null and bid_list_price_usd = 0 then bid_item_list_price_usd
            else bid_list_price_usd
         end as bid_list_price_usd

        ,case
            when promotion_id = 11 then 0
            else bid_price_paid_usd
         end as bid_price_paid_usd
        
        ,case
            when promotion_id is not null and bid_member_price_usd = 0 then bid_item_member_price_usd
            else bid_member_price_usd
         end as bid_member_price_usd

        ,case
            when promotion_id is not null and bid_non_member_price_usd = 0 then bid_item_non_member_price_usd
            else bid_non_member_price_usd
         end as bid_non_member_price_usd

        ,bid_item_credit_usd
        ,is_fulfillment_at_risk
        ,bid_quantity
        ,created_at_utc
        ,updated_at_utc
        ,first_stuck_at_utc
        ,used_member_pricing
    from order_item_joins
)

,order_item_revenue_calculations as (
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,promotion_id
        ,promotion_source
        ,bid_token
        ,product_id
        ,product_name
        ,bid_item_name
        ,bid_item_type
        ,bid_item_subtype
        ,autofill_reason
        ,fill_type
        ,bid_list_price_usd
        ,bid_price_paid_usd
        ,bid_list_price_usd * bid_quantity as bid_gross_product_revenue
        ,(bid_list_price_usd - bid_price_paid_usd) * bid_quantity as total_order_item_discount
        ,bid_non_member_price_usd
        ,bid_member_price_usd
        ,bid_item_credit_usd
        ,is_fulfillment_at_risk
        ,bid_quantity
        ,created_at_utc
        ,updated_at_utc
        ,first_stuck_at_utc
        ,used_member_pricing
    from update_promotion_bid_prices
)

,order_item_discount_calculations as (
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,promotion_id
        ,promotion_source
        ,bid_token
        ,product_id
        ,product_name
        ,bid_item_name
        ,bid_item_type
        ,bid_item_subtype
        ,autofill_reason
        ,fill_type
        ,bid_list_price_usd
        ,bid_price_paid_usd
        ,bid_gross_product_revenue
        ,total_order_item_discount

        ,round(
            case
                when promotion_id is null and total_order_item_discount > 0 and used_member_pricing then (bid_list_price_usd * bid_quantity) * .05  -- Standard member discount is 5%
                else 0
            end
        ,2) as item_member_discount
    
        ,round(
            case
                when promotion_id is null and total_order_item_discount > 0 and used_member_pricing
                    then ((bid_list_price_usd - bid_price_paid_usd) * bid_quantity) - ((bid_list_price_usd * bid_quantity) * .05) -- Anything above the standard 5% member discount should be considered a merch discount
                when promotion_id is null and total_order_item_discount > 0 and not used_member_pricing
                    then ((bid_list_price_usd - bid_price_paid_usd) * bid_quantity)
                else 0
            end
        ,2) as item_merch_discount

        ,round(
            case
                when (promotion_id in (18,20,22,35,37) and promotion_source = 'PROMOTION') 
                or (promotion_id in (259, 285, 286, 287, 299, 300, 301) and promotion_source = 'PROMOTIONS::PROMOTION')  then total_order_item_discount
                else 0
            end
        ,2) as item_free_protein_discount
    
        ,round(
            case
                when (promotion_id not in (18,20,22,35,37) and promotion_source = 'PROMOTION' and promotion_id is not null)
                or (promotion_id not in (259, 285, 286, 287, 299, 300, 301) and promotion_source = 'PROMOTIONS::PROMOTION' and promotion_id is not null )
                then total_order_item_discount + bid_item_credit_usd
                else 0
            end
        ,2) as item_promotion_discount

        ,bid_member_price_usd
        ,bid_non_member_price_usd
        ,bid_item_credit_usd
        ,is_fulfillment_at_risk
        ,bid_quantity
        ,created_at_utc
        ,updated_at_utc
        ,first_stuck_at_utc
    from order_item_revenue_calculations
)

select * from order_item_discount_calculations

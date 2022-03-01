with

order_item as ( select * from {{ ref('order_items') }} )
,bid_item_sku as ( select * from {{ ref('int_bid_item_skus') }} )
,sku as ( select * from {{ ref('skus') }} )

,join_bid_item_skus as (
    select  
        order_item.order_id
        ,order_item.bid_id
        ,order_item.bid_item_id
        ,order_item.promotion_id
        ,order_item.bid_item_name
        ,order_item.bid_quantity
        ,order_item.bid_list_price_usd
        ,order_item.bid_gross_product_revenue
        ,order_item.item_member_discount as item_member_discount
        ,order_item.item_merch_discount as item_merch_discount
        ,order_item.item_promotion_discount as item_promotion_discount
        ,order_item.created_at_utc as bid_created_at_utc
        ,order_item.updated_at_utc as bid_updated_at_utc
        ,bid_item_sku.sku_id
        ,bid_item_sku.sku_quantity
        ,bid_item_sku.is_single_sku_bid_item
        ,order_item.bid_quantity * bid_item_sku.sku_quantity as bid_sku_quantity
    from order_item
        left join bid_item_sku on order_item.bid_item_id = bid_item_sku.bid_item_id
            and order_item.created_at_utc >= bid_item_sku.adjusted_dbt_valid_from
            and order_item.created_at_utc < bid_item_sku.adjusted_dbt_valid_to
)

,join_historical_sku_info as (
    select
        join_bid_item_skus.*
        ,sku.sku_key
        ,sku.sku_price_usd
        ,sku.sku_cost_usd
        ,sku.is_marketplace
        
        ,case
            when div0(sku.sku_price_usd * join_bid_item_skus.bid_sku_quantity,join_bid_item_skus.bid_gross_product_revenue) > 1 
                or join_bid_item_skus.sku_id is null then 1
            else div0(sku.sku_price_usd * join_bid_item_skus.bid_sku_quantity,join_bid_item_skus.bid_gross_product_revenue)
         end as sku_price_proportion

    from join_bid_item_skus
        left join sku on join_bid_item_skus.sku_id = sku.sku_id
            and join_bid_item_skus.bid_created_at_utc >= sku.adjusted_dbt_valid_from
            and join_bid_item_skus.bid_created_at_utc < sku.adjusted_dbt_valid_to
)

,sku_proportion_calculations as (
    select
        *
        ,sku_price_usd * bid_sku_quantity as bid_sku_price
        ,sku_cost_usd * bid_sku_quantity as bid_sku_cost
        ,round(
            case
                when is_single_sku_bid_item or sku_id is null then bid_gross_product_revenue
                else sku_price_proportion * bid_gross_product_revenue
            end 
        ,2) as sku_gross_product_revenue

        ,round(
            case
                when is_single_sku_bid_item or sku_id is null then item_member_discount
                else sku_price_proportion * item_member_discount
            end
        ,2) as sku_membership_discount

        ,round(
            case
                when is_single_sku_bid_item or sku_id is null then item_merch_discount
                else sku_price_proportion * item_merch_discount
            end
         ,2) as sku_merch_discount

        ,round(
            case
                when is_single_sku_bid_item or sku_id is null then item_promotion_discount
                else sku_price_proportion * item_promotion_discount
            end
         ,2) as sku_free_protein_promotion

    from join_historical_sku_info
)

,final as (
    select
        {{ dbt_utils.surrogate_key( ['order_id','bid_id','bid_item_id','sku_id'] ) }} as order_item_details_id
        ,order_id
        ,bid_id
        ,bid_item_id
        ,sku_id
        ,sku_key
        ,promotion_id
        ,bid_item_name
        ,bid_quantity
        ,sku_quantity
        ,bid_sku_quantity
        ,bid_list_price_usd
        ,sku_price_usd
        ,sku_cost_usd
        ,bid_sku_price
        ,bid_sku_cost
        ,sku_price_proportion
        ,sku_gross_product_revenue
        ,sku_membership_discount
        ,sku_merch_discount
        ,sku_free_protein_promotion
        
        ,sku_gross_product_revenue
         + sku_membership_discount
         + sku_merch_discount
         + sku_free_protein_promotion
        as sku_net_product_revenue

        ,is_single_sku_bid_item
        ,is_marketplace
        ,bid_created_at_utc
        ,bid_updated_at_utc
    from sku_proportion_calculations
)

select * from final

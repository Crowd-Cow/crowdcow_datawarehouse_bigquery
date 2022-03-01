with

ordered_items as ( select * from {{ ref('int_ordered_skus') }} )
,sku as ( select * from {{ ref('skus') }} )

,sku_proportion_calculations as (
    select
        *
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

    from ordered_items
)

,final as (
    select
        {{ dbt_utils.surrogate_key( ['order_id','bid_id','bid_item_id','sku_id'] ) }} as order_item_details_id
        ,order_id
        ,bid_id
        ,bid_item_id
        ,sku_id
        ,ordered_sku_key
        ,promotion_id
        ,bid_item_name
        ,bid_quantity
        ,sku_quantity
        ,bid_sku_quantity
        ,bid_list_price_usd
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
        ,bid_created_at_utc
        ,bid_updated_at_utc
    from sku_proportion_calculations
)

select * from final

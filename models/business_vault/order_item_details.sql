with

ordered_items as ( select * from {{ ref('int_ordered_skus') }} )
,packed_items as ( select * from {{ ref('int_packed_skus') }} )
,packed_not_ordered as ( select * from {{ ref('int_packed_skus') }} where is_packed_item_only )

,aggregate_packed_bid_items as (
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,sum(packed_sku_quantity) as packed_bid_item_quantity
        ,sum(packed_sku_cost) as packed_sku_cost
        ,max(packed_created_at_utc) as packed_created_at_utc
        ,max(packed_updated_at_utc) as packed_updated_at_utc
    from packed_items
    where not is_packed_item_only
    group by 1,2,3
)

,join_ordered_packed as (
    select
        ordered_items.order_id
        ,ordered_items.bid_id
        ,ordered_items.bid_item_id
        ,ordered_items.promotion_id
        ,ordered_items.bid_item_name
        ,ordered_items.bid_quantity
        ,ordered_items.bid_list_price_usd
        ,ordered_items.bid_gross_product_revenue
        ,ordered_items.item_member_discount
        ,ordered_items.item_merch_discount
        ,ordered_items.item_promotion_discount
        ,ordered_items.bid_created_at_utc
        ,ordered_items.bid_updated_at_utc
        ,ordered_items.sku_id
        ,ordered_items.ordered_sku_key
        ,ordered_items.bid_sku_price
        ,coalesce(aggregate_packed_bid_items.packed_sku_cost,ordered_items.bid_sku_cost) as bid_sku_cost
        ,ordered_items.sku_price_proportion
        ,ordered_items.sku_quantity_proportion
        ,ordered_items.is_single_sku_bid_item
        ,ordered_items.bid_sku_quantity
        ,aggregate_packed_bid_items.order_id is not null as is_item_packed
        ,aggregate_packed_bid_items.packed_bid_item_quantity
        ,aggregate_packed_bid_items.packed_created_at_utc
        ,aggregate_packed_bid_items.packed_updated_at_utc
    from ordered_items
        left join aggregate_packed_bid_items on ordered_items.order_id = aggregate_packed_bid_items.order_id
            and ordered_items.bid_id = aggregate_packed_bid_items.bid_id
            and ordered_items.bid_item_id = aggregate_packed_bid_items.bid_item_id
)

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

        ,round(
            case
                when is_single_sku_bid_item or sku_id is null then bid_sku_cost
                else sku_price_proportion * bid_sku_cost
            end 
        ,2) as sku_cost

        ,round(
            case
                when is_single_sku_bid_item or sku_id is null then packed_bid_item_quantity
                else sku_quantity_proportion * packed_bid_item_quantity
            end 
        ,0) as packed_sku_quantity

    from join_ordered_packed
)

,final as (
    select
        {{ dbt_utils.surrogate_key( ['order_id','bid_id','bid_item_id','sku_id'] ) }} as order_item_details_id
        ,order_id
        ,bid_id
        ,bid_item_id
        ,sku_id
        ,ordered_sku_key as sku_key
        ,promotion_id
        ,bid_item_name
        ,bid_quantity
        ,bid_sku_quantity
        ,packed_sku_quantity
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

        ,sku_cost

        ,is_single_sku_bid_item
        ,is_item_packed
        ,bid_created_at_utc
        ,bid_updated_at_utc
        ,packed_created_at_utc
        ,packed_updated_at_utc
    from sku_proportion_calculations

    union all

    select
        {{ dbt_utils.surrogate_key( ['order_id','bid_id','bid_item_id','sku_id'] ) }} as order_item_details_id
        ,order_id
        ,bid_id
        ,bid_item_id
        ,sku_id
        ,packed_sku_key as sku_key
        ,null::int promotion_id
        ,null::text as bid_item_name
        ,packed_sku_quantity as bid_quantity
        ,packed_sku_quantity as bid_sku_quantity
        ,packed_sku_quantity
        ,packed_sku_price as bid_list_price_usd
        ,packed_sku_price as bid_sku_price
        ,packed_sku_cost as bid_sku_cost
        ,1 as sku_price_proportion
        ,0 as sku_gross_product_revenue
        ,0 as sku_membership_discount
        ,0 as sku_merch_discount
        ,0 as sku_free_protein_promotion
        ,0 as sku_net_product_revenue
        ,packed_sku_cost as sku_cost
        ,TRUE as is_single_sku_bid_item
        ,TRUE as is_item_packed
        ,null::timestamp as bid_created_at_utc
        ,null::timestamp as bid_updated_at_utc
        ,packed_created_at_utc
        ,packed_updated_at_utc
    from packed_not_ordered
)

select * from final

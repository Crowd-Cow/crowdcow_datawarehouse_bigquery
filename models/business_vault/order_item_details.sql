with

ordered_items as ( select * from {{ ref('int_ordered_skus') }} )
,packed_items as ( select * from {{ ref('int_packed_skus') }} )
,vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,sku as ( select * from {{ ref('stg_cc__skus') }} )
,completed_orders as ( select order_id from {{ ref('stg_cc__orders') }} where order_current_state in ('COMPLETE','FULLY_PACKED','FULLY_PACKED_AS_IS','SHIP_AS_IS'))
,non_gift_orders as ( select order_id from {{ ref('int_order_flags') }} where not is_gift_card_order )
,receivable as ( select * from {{ ref('stg_cc__pipeline_receivables') }} )
,pipeline_order as ( select * from {{ ref('stg_cc__pipeline_orders') }} )

,union_skus as (
    select 
        packed_items.*
    from packed_items
        inner join completed_orders on packed_items.order_id = completed_orders.order_id
        inner join non_gift_orders on packed_items.order_id = non_gift_orders.order_id
    
    union all
    
    select 
        ordered_items.order_item_detail_id
        ,ordered_items.order_id
        ,ordered_items.bid_id
        ,ordered_items.bid_item_id
        ,ordered_items.sku_id
        ,ordered_items.sku_key

        /*** The SKU box and lot information for an order item is not known until the order is packed. ***/
        ,null::int as sku_box_id
        ,null::text as sku_box_key
        ,null::int as sku_owner_id
        ,null::text as lot_number

        ,ordered_items.fc_id
        ,ordered_items.fc_key
        ,ordered_items.promotion_id
        ,ordered_items.bid_item_name
        ,ordered_items.bid_quantity
        ,ordered_items.sku_quantity
        ,ordered_items.bid_list_price_usd
        ,ordered_items.bid_gross_product_revenue
        ,ordered_items.item_member_discount
        ,ordered_items.item_merch_discount
        ,ordered_items.item_promotion_discount
        ,ordered_items.is_single_sku_bid_item
        ,false as is_item_packed
        ,false as was_manually_changed
        ,ordered_items.created_at_utc
        ,ordered_items.updated_at_utc
        ,null::timestamp as packed_created_at_utc      

    from ordered_items left join packed_items on ordered_items.order_id = packed_items.order_id 
        and ordered_items.bid_id = packed_items.bid_id 
        and ordered_items.bid_item_id = packed_items.bid_item_id
    where packed_items.order_id is null
)

,get_owner_details as (
    select
        skus.*
        ,vendor.sku_vendor_name as owner_name
        ,coalesce(vendor.is_marketplace,TRUE) as is_marketplace
    from union_skus as skus
        left join vendor on skus.sku_owner_id = vendor.sku_vendor_id
)

,get_lot_cost_per_unit as (
    select distinct
        receivable.sku_id
        ,pipeline_order.lot_number
        ,receivable.pipeline_order_id
        ,receivable.cost_per_unit_usd
    from receivable
        inner join pipeline_order on receivable.pipeline_order_id = pipeline_order.pipeline_order_id
    where receivable.marked_destroyed_at_utc is null
)

,get_sku_financials as (
    select
        get_owner_details.*
        ,iff(sku_price_usd = 0 and bid_list_price_usd > 0 and is_single_sku_bid_item,bid_list_price_usd,sku_price_usd) as sku_price_usd

        ,case
            when get_owner_details.is_marketplace and get_lot_cost_per_unit.cost_per_unit_usd is null then coalesce(nullif(sku.marketplace_cost_usd,0),sku.owned_sku_cost_usd)
            when not get_owner_details.is_marketplace and get_lot_cost_per_unit.cost_per_unit_usd is null then sku.owned_sku_cost_usd
            else get_lot_cost_per_unit.cost_per_unit_usd
         end as sku_cost_usd

    from get_owner_details
        left join sku on get_owner_details.sku_key = sku.sku_key
        left join get_lot_cost_per_unit on get_owner_details.sku_id = get_lot_cost_per_unit.sku_id
            and get_owner_details.lot_number = get_lot_cost_per_unit.lot_number
)

,calculate_sku_price_proportion as (
    select
        get_sku_financials.*
    
        ,case
            when div0(sku_price_usd * sku_quantity,bid_gross_product_revenue) > 1
                or sku_id is null
                or (was_manually_changed and sku_quantity = 0) then 1
            when sku_quantity = 0 then 0
            else div0(sku_price_usd * sku_quantity,bid_gross_product_revenue)
         end as sku_price_proportion
    
    from get_sku_financials
)

,calculate_sku_revenue as (
    select *
    ,round(
        case
            when (not is_item_packed and is_single_sku_bid_item) or sku_id is null then bid_gross_product_revenue
            else sku_price_proportion * bid_gross_product_revenue
        end 
        ,2) as sku_gross_product_revenue
    ,round(
        case
            when (not is_item_packed and is_single_sku_bid_item) or sku_id is null then item_member_discount
            else sku_price_proportion * item_member_discount
        end 
        ,2) * -1 as sku_member_discount
    ,round(
        case
            when (not is_item_packed and is_single_sku_bid_item) or sku_id is null then item_merch_discount
            else sku_price_proportion * item_merch_discount
        end 
        ,2) * -1 as sku_merch_discount
    ,round(
        case
            when (not is_item_packed and is_single_sku_bid_item) or sku_id is null then item_promotion_discount
            else sku_price_proportion * item_promotion_discount
        end 
        ,2) * -1 as sku_promotion_discount
    from calculate_sku_price_proportion
)

,final as (
    select
    order_item_detail_id
    ,order_id
    ,bid_id
    ,bid_item_id
    ,sku_id
    ,sku_key
    ,sku_box_id
    ,sku_box_key
    ,sku_owner_id
    ,lot_number
    ,fc_id
    ,fc_key
    ,promotion_id
    ,owner_name
    ,bid_item_name
    ,bid_quantity
    ,sku_quantity
    ,sku_price_usd as sku_price
    ,sku_cost_usd as sku_cost
    ,sku_price_proportion
    ,sku_gross_product_revenue
    ,sku_member_discount
    ,sku_merch_discount
    ,sku_promotion_discount
    
    ,sku_gross_product_revenue
        + sku_member_discount
        + sku_merch_discount
        + sku_promotion_discount
    as sku_net_product_revenue
    
    ,is_item_packed
    ,is_marketplace
    ,is_single_sku_bid_item
    ,created_at_utc
    ,updated_at_utc
    ,packed_created_at_utc
from calculate_sku_revenue
)

select * from final

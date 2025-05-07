with

ordered_items as ( select * from {{ ref('int_ordered_skus') }} )
,packed_items as ( select * from {{ ref('int_packed_skus') }} )
,vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,sku as ( select * from {{ ref('skus') }} )
,completed_orders as ( select order_id from {{ ref('stg_cc__orders') }} where (order_current_state in ('COMPLETE','FULLY_PACKED','FULLY_PACKED_AS_IS','SHIP_AS_IS') or order_id in (3331204,3335959,3335952,3335958,3359918,3536280,3536266,3536290,3537158,3536275) ) ) --hardcoded byb orders
,non_gift_orders as ( select order_id from {{ ref('int_order_flags') }} where not is_gift_card_order )
,receivable as ( select * from {{ ref('stg_cc__pipeline_receivables') }} )
,pipeline_order as ( select * from {{ ref('stg_cc__pipeline_orders') }} )
,current_sku_reservation as (
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,sku_id
    from {{ ref('stg_cc__sku_reservations') }}
    where dbt_valid_to is null
        and order_id is not null
        and bid_id is not null
        and bid_item_id is not null
        and sku_id is not null
    qualify row_number() over(partition by order_id,bid_id,bid_item_id,sku_id order by dbt_valid_from desc) = 1
)

,get_packed_orders as (
    select
        packed_items.*
    from packed_items
        inner join completed_orders on packed_items.order_id = completed_orders.order_id
        inner join non_gift_orders on packed_items.order_id = non_gift_orders.order_id
)

,union_skus as (
    select *
    from get_packed_orders
    
    union all
    
    select 
        ordered_items.order_item_detail_id
        ,ordered_items.order_id
        ,ordered_items.bid_id
        ,ordered_items.bid_item_id
        ,ordered_items.sku_id
        ,ordered_items.sku_key
        ,ordered_items.sku_key as bid_sku_key

        /*** The SKU box and lot information for an order item is not known until the order is packed. ***/
        ,cast(null as int64 )as sku_box_id
        ,cast(null as string) as sku_box_key
        ,cast(null as int64 )as sku_owner_id
        ,cast(null as string) as lot_number

        ,ordered_items.fc_id
        ,ordered_items.fc_key
        ,ordered_items.promotion_id
        ,ordered_items.promotion_source
        ,ordered_items.bid_item_name
        ,ordered_items.product_title
        ,ordered_items.bid_quantity
        ,ordered_items.sku_quantity
        ,ordered_items.bid_list_price_usd
        ,ordered_items.bid_gross_product_revenue
        ,ordered_items.item_member_discount
        ,ordered_items.item_merch_discount
        ,ordered_items.item_free_protein_discount
        ,ordered_items.item_promotion_discount
        ,ordered_items.is_single_sku_bid_item
        ,false as is_item_packed
        ,false as was_manually_changed
        ,ordered_items.created_at_utc
        ,ordered_items.bid_created_at_utc
        ,ordered_items.updated_at_utc
        ,cast(null as timestamp) as packed_created_at_utc      

    from ordered_items left join get_packed_orders on ordered_items.order_id = get_packed_orders.order_id 
        and ordered_items.bid_id = get_packed_orders.bid_id 
        and ordered_items.bid_item_id = get_packed_orders.bid_item_id
    where get_packed_orders.order_id is null
)

,get_owner_details as (
    select
        skus.*
        ,vendor.sku_vendor_name as owner_name
        ,coalesce(vendor.is_marketplace,TRUE) as is_marketplace
        ,coalesce(vendor.is_rastellis,FALSE) as is_rastellis
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
        ,if(sku_price_usd = 0 and bid_list_price_usd > 0 and is_single_sku_bid_item,bid_list_price_usd,sku_price_usd) as sku_price_usd

        ,case
            when get_owner_details.is_marketplace and nullif(get_lot_cost_per_unit.cost_per_unit_usd,0) is null then coalesce(nullif(sku.marketplace_cost_usd,0),sku.owned_sku_cost_usd)
            when not get_owner_details.is_marketplace and nullif(get_lot_cost_per_unit.cost_per_unit_usd,0) is null then sku.owned_sku_cost_usd
            else get_lot_cost_per_unit.cost_per_unit_usd
         end as sku_cost_usd

        ,sku_quantity * sku_weight as total_sku_weight

    from get_owner_details
        left join sku on get_owner_details.sku_key = sku.sku_key
        left join get_lot_cost_per_unit on get_owner_details.sku_id = get_lot_cost_per_unit.sku_id
            and get_owner_details.lot_number = get_lot_cost_per_unit.lot_number
)

,calculate_sku_price_proportion as (
    select
        get_sku_financials.*
    
        ,case
            when safe_divide(sku_price_usd * sku_quantity,bid_gross_product_revenue) > 1
                or sku_id is null
                or (was_manually_changed and sku_quantity = 0) then 1
            when sku_quantity = 0 then 0
            else safe_divide(sku_price_usd * sku_quantity,bid_gross_product_revenue)
         end as sku_price_proportion
    
    from get_sku_financials
)

,calculate_sku_revenue as (
    select 
    *
    
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
            when (not is_item_packed and is_single_sku_bid_item) or sku_id is null then item_free_protein_discount
            else sku_price_proportion * item_free_protein_discount
        end 
        ,2) * -1 as sku_free_protein_discount
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
        calculate_sku_revenue.order_item_detail_id
        ,calculate_sku_revenue.order_id
        ,calculate_sku_revenue.bid_id
        ,calculate_sku_revenue.bid_item_id
        ,calculate_sku_revenue.sku_id
        ,calculate_sku_revenue.sku_key
        ,calculate_sku_revenue.bid_sku_key
        ,calculate_sku_revenue.sku_box_id
        ,calculate_sku_revenue.sku_box_key
        ,calculate_sku_revenue.sku_owner_id
        ,calculate_sku_revenue.lot_number
        ,calculate_sku_revenue.fc_id
        ,calculate_sku_revenue.fc_key
        ,calculate_sku_revenue.promotion_id
        ,calculate_sku_revenue.promotion_source
        ,calculate_sku_revenue.owner_name
        ,calculate_sku_revenue.bid_item_name
        ,calculate_sku_revenue.product_title
        ,calculate_sku_revenue.bid_quantity
        ,calculate_sku_revenue.sku_quantity
        ,calculate_sku_revenue.total_sku_weight
        ,calculate_sku_revenue.sku_price_usd as sku_price
        ,calculate_sku_revenue.sku_cost_usd as sku_cost
        ,calculate_sku_revenue.sku_price_proportion
        ,calculate_sku_revenue.sku_gross_product_revenue
        ,calculate_sku_revenue.sku_member_discount
        ,calculate_sku_revenue.sku_merch_discount
        ,calculate_sku_revenue.sku_free_protein_discount
        ,calculate_sku_revenue.sku_promotion_discount
        
        ,calculate_sku_revenue.sku_gross_product_revenue
            + calculate_sku_revenue.sku_member_discount
            + calculate_sku_revenue.sku_merch_discount
            + calculate_sku_revenue.sku_free_protein_discount
            + calculate_sku_revenue.sku_promotion_discount
        as sku_net_product_revenue
        
        ,calculate_sku_revenue.is_item_packed
        ,calculate_sku_revenue.is_marketplace
        ,calculate_sku_revenue.is_single_sku_bid_item
        ,calculate_sku_revenue.is_rastellis
        ,current_sku_reservation.order_id is not null as is_reserved
        ,calculate_sku_revenue.created_at_utc
        ,calculate_sku_revenue.bid_created_at_utc
        ,calculate_sku_revenue.updated_at_utc
        ,calculate_sku_revenue.packed_created_at_utc
    from calculate_sku_revenue
        left join current_sku_reservation on calculate_sku_revenue.order_id = current_sku_reservation.order_id
            and calculate_sku_revenue.bid_id = current_sku_reservation.bid_id
            and calculate_sku_revenue.bid_item_id = current_sku_reservation.bid_item_id
            and calculate_sku_revenue.sku_id = current_sku_reservation.sku_id
)

select * from final

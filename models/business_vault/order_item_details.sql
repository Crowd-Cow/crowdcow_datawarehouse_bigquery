with

order_item as ( select * from {{ ref('order_items') }})
,bid_item_skus as ( select * from {{ ref('stg_cc__bid_item_sku_with_quantities') }} )
,sku as ( select * from {{ ref('skus') }} )
,order_packed_skus as ( select * from {{ ref('stg_cc__order_packed_skus') }} )
,sku_reservation as ( select * from {{ ref('stg_cc__sku_reservations') }} where dbt_valid_to is null )

,packed_orders as (
    select distinct order_id
    from order_packed_skus
)

,packed_sku_reservations as (
    select
        order_id
        ,sku_id
        ,sku_reservation_id
        ,sum(sku_quantity) as sku_quantity
    from order_packed_skus
    group by 1,2,3
)

,order_item_skus as (
    select
        order_item.order_id
        ,order_item.bid_id
        ,order_item.bid_item_id
        ,order_item.product_id
        ,bid_item_skus.sku_id as ordered_sku_id
        ,order_item.product_name
        ,order_item.bid_item_name
        ,order_item.bid_item_type
        ,order_item.bid_item_subtype
        ,order_item.bid_quantity * bid_item_skus.sku_quantity as ordered_sku_quantity
        ,order_item.created_at_utc
    from order_item
        left join bid_item_skus on order_item.bid_item_id = bid_item_skus.bid_item_id
)

,packed_skus as (

    /*** SKU reservations without a bid ID are primarily items like inserts, handwritten notes, etc. ***/
    /** This CTE excludes those items without a bid ID ***/

    select
        packed_sku_reservations.order_id
        ,sku_reservation.bid_id
        ,sku_reservation.bid_item_id
        ,packed_sku_reservations.sku_id as packed_sku_id
        ,sku_reservation.sku_id as reserved_sku_id
        ,sum(sku_reservation.original_quantity) as packed_sku_quantity
    from packed_sku_reservations
        left join sku_reservation on packed_sku_reservations.sku_reservation_id = sku_reservation.sku_reservation_id
    where sku_reservation.bid_id is not null
    group by 1,2,3,4,5
)

,join_common_packed_skus as (

    /*** Joins SKUs for a bid item that were ordered with the packed SKUs ***/
    /*** where the packed SKUs for a bid item are the same as what was originally ordered ***/

    select
        order_item_skus.order_id
        ,order_item_skus.bid_id
        ,order_item_skus.bid_item_id
        ,order_item_skus.product_id
        ,order_item_skus.product_name
        ,order_item_skus.bid_item_name
        ,order_item_skus.bid_item_type
        ,order_item_skus.bid_item_subtype
        ,order_item_skus.ordered_sku_id
        ,ordered_sku_quantity
        ,packed_orders.order_id is not null as is_order_packed
        ,packed_skus.packed_sku_id is not null as is_ordered_sku_packed
        ,order_item_skus.created_at_utc
    from order_item_skus
        left join packed_orders on order_item_skus.order_id = packed_orders.order_id
        left join packed_skus on order_item_skus.order_id = packed_skus.order_id
            and order_item_skus.bid_id = packed_skus.bid_id
            and order_item_skus.bid_item_id = packed_skus.bid_item_id
            and order_item_skus.ordered_sku_id = packed_skus.packed_sku_id
)

,find_pack_swap_skus as (

    /*** Some SKUs for a bid item are swapped when an order is packed. This CTE creates a data set of ***/
    /*** all packed SKUs that are different than the SKUs that were originally ordered ***/

    select
        order_id
        ,bid_id
        ,bid_item_id
        ,packed_sku_id
        ,packed_sku_quantity
    from packed_skus
    
    except
    
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,ordered_sku_id
        ,ordered_sku_quantity
    from order_item_skus
)

,add_pack_swap_skus as (

    /*** Joins the ordered SKUs for a bid item that were the same as the packed SKUs ***/
    /*** with the packed SKUs that were swapped for a given bid item at the time of packing ***/

    select
        join_common_packed_skus.order_id
        ,join_common_packed_skus.bid_id
        ,join_common_packed_skus.bid_item_id
        ,join_common_packed_skus.product_id
        ,join_common_packed_skus.ordered_sku_id
        ,coalesce(find_pack_swap_skus.packed_sku_id, join_common_packed_skus.ordered_sku_id) as packed_sku_id
        ,join_common_packed_skus.bid_item_type
        ,join_common_packed_skus.bid_item_subtype
        ,join_common_packed_skus.product_name
        ,join_common_packed_skus.bid_item_name
        ,join_common_packed_skus.ordered_sku_quantity
        ,coalesce(find_pack_swap_skus.packed_sku_quantity, join_common_packed_skus.ordered_sku_quantity) as packed_sku_quantity
        ,join_common_packed_skus.is_order_packed
        ,join_common_packed_skus.is_ordered_sku_packed
        ,join_common_packed_skus.created_at_utc
    from join_common_packed_skus
        left join find_pack_swap_skus on join_common_packed_skus.order_id = find_pack_swap_skus.order_id
            and join_common_packed_skus.bid_id = find_pack_swap_skus.bid_id
            and join_common_packed_skus.bid_item_id = find_pack_swap_skus.bid_item_id
            
)

,add_sku_details as (
    select
        add_pack_swap_skus.order_id
        ,add_pack_swap_skus.bid_id
        ,add_pack_swap_skus.bid_item_id
        ,add_pack_swap_skus.product_id
        ,add_pack_swap_skus.ordered_sku_id
        ,add_pack_swap_skus.packed_sku_id
        ,add_pack_swap_skus.bid_item_type
        ,add_pack_swap_skus.bid_item_subtype
        ,add_pack_swap_skus.product_name
        ,add_pack_swap_skus.bid_item_name
        ,sku.farm_name
        ,sku.category
        ,sku.sub_category
        ,sku.cut_name
        ,packed_sku.farm_name as packed_farm_name
        ,packed_sku.category as packed_category
        ,packed_sku.sub_category as packed_sub_category
        ,packed_sku.cut_name as packed_cut_name
        ,add_pack_swap_skus.ordered_sku_quantity
        ,add_pack_swap_skus.packed_sku_quantity
        ,add_pack_swap_skus.is_order_packed
        ,add_pack_swap_skus.is_ordered_sku_packed
        ,add_pack_swap_skus.created_at_utc
    from add_pack_swap_skus
        left join sku on add_pack_swap_skus.ordered_sku_id = sku.sku_id
        left join sku as packed_sku on add_pack_swap_skus.packed_sku_id = packed_sku.sku_id
)


select * from add_sku_details

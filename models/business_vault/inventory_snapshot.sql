with

/*** Only starting with dates after 2021-10-28 since that is when we started fully snapshotting the `sku_boxes` data in the new model ***/
dates as ( select calendar_date from {{ ref('stg_reference__date_spine') }} where calendar_date >= '2021-10-28' and calendar_date <= sysdate()::date )
,sku_box as ( select * from {{ ref('stg_cc__sku_boxes') }} )
,fc_location as ( select * from {{ ref('stg_cc__fc_locations') }} )
,fc as ( select * from {{ ref('fcs') }} )
,sku as ( select * from {{ ref('skus') }} )
,lot as ( select * from {{ ref('lots') }} )
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )

,inventory_snapshot as (
    select
        sku_box_id
        ,sku_box_key
        ,fc_id
        ,sku_id
        ,owner_id
        ,lot_id
        ,pallet_id
        ,sku_box_name
        ,min_weight
        ,max_weight
        ,quantity
        ,quantity_reserved
        ,quantity - quantity_reserved as quantity_available
        ,quarantined_quantity
        ,fc_location_parent_id
        ,created_at_utc
        ,updated_at_utc
        ,marked_not_for_sale_at_utc
        ,marked_destroyed_at_utc
        ,delivered_at_utc
        ,moved_to_picking_at_utc
        ,dbt_valid_from::date as dbt_valid_from
        ,coalesce(dbt_valid_to,sysdate()::date + 1)::date as dbt_valid_to
        ,row_number() over(partition by sku_box_id, dbt_valid_from::date order by dbt_valid_from desc) as rn
    from sku_box
    qualify rn = 1
    order by dbt_valid_from
)

,daily_sku_boxes as (
    select
        dates.calendar_date as snapshot_date
        ,inventory_snapshot.sku_box_id
        ,inventory_snapshot.sku_box_key
        ,inventory_snapshot.fc_id
        ,inventory_snapshot.sku_id
        ,inventory_snapshot.owner_id
        ,inventory_snapshot.lot_id
        ,inventory_snapshot.pallet_id
        ,inventory_snapshot.sku_box_name
        ,inventory_snapshot.min_weight
        ,inventory_snapshot.max_weight
        ,inventory_snapshot.quantity
        ,inventory_snapshot.quantity_reserved
        ,inventory_snapshot.quantity - quantity_reserved as quantity_available
        ,inventory_snapshot.quarantined_quantity
        ,inventory_snapshot.fc_location_parent_id
        ,inventory_snapshot.marked_destroyed_at_utc is not null as is_destroyed
        ,inventory_snapshot.created_at_utc
        ,inventory_snapshot.updated_at_utc
        ,inventory_snapshot.marked_not_for_sale_at_utc
        ,inventory_snapshot.marked_destroyed_at_utc
        ,inventory_snapshot.delivered_at_utc
        ,inventory_snapshot.moved_to_picking_at_utc
    from dates
        left join inventory_snapshot on dates.calendar_date >= inventory_snapshot.dbt_valid_from
            and dates.calendar_date < inventory_snapshot.dbt_valid_to
)

,sku_box_locations as (
    select 
        daily_sku_boxes.snapshot_date
        ,daily_sku_boxes.sku_box_id
        ,daily_sku_boxes.sku_box_key
        ,daily_sku_boxes.fc_id
        ,daily_sku_boxes.sku_id
        ,daily_sku_boxes.owner_id
        ,daily_sku_boxes.lot_id
        ,daily_sku_boxes.pallet_id
        ,daily_sku_boxes.fc_location_parent_id as fc_location_id
        ,daily_sku_boxes.sku_box_name
        ,fc_location.location_type
        ,fc_location.location_name
        ,daily_sku_boxes.min_weight
        ,daily_sku_boxes.max_weight
        ,daily_sku_boxes.quantity
        ,daily_sku_boxes.quantity_reserved
        ,daily_sku_boxes.quantity_available
        ,daily_sku_boxes.quarantined_quantity
        
        ,case 
            when fc_location.is_sellable then daily_sku_boxes.quantity_available  
            else 0 
         end as quantity_sellable
        
        ,fc_location.is_sellable
        ,daily_sku_boxes.is_destroyed
        ,daily_sku_boxes.created_at_utc
        ,daily_sku_boxes.updated_at_utc
        ,daily_sku_boxes.marked_not_for_sale_at_utc
        ,daily_sku_boxes.marked_destroyed_at_utc
        ,daily_sku_boxes.delivered_at_utc
        ,daily_sku_boxes.moved_to_picking_at_utc
    from daily_sku_boxes
        left join fc_location on daily_sku_boxes.fc_location_parent_id = fc_location.fc_location_id
)

,inventory_joins as (
    select 
        {{ dbt_utils.surrogate_key(['snapshot_date','sku_box_key'] ) }} as inventory_snapshot_id
        ,snapshot_date
        ,sku_box_id
        ,sku_box_key
        ,sku_box_locations.fc_id
        ,fc.fc_key
        ,sku_box_locations.sku_id
        ,sku.sku_key
        ,sku_box_locations.owner_id
        ,sku_box_locations.lot_id
        ,lot.lot_key
        ,sku_box_locations.pallet_id
        ,sku_box_locations.fc_location_id
        ,sku_box_locations.sku_box_name
        ,sku_vendor.sku_vendor_name as sku_box_owner_name
        ,sku_box_locations.location_type
        ,sku_box_locations.location_name
        ,sku_box_locations.min_weight
        ,sku_box_locations.max_weight
        ,sku_box_locations.quantity
        ,sku_box_locations.quantity_reserved
        ,sku_box_locations.quantity_available
        ,sku_box_locations.quarantined_quantity
        ,sku_box_locations.quantity_sellable
        ,sku_box_locations.is_sellable
        ,sku_box_locations.is_destroyed
        ,sku_box_locations.created_at_utc
        ,sku_box_locations.updated_at_utc
        ,sku_box_locations.marked_not_for_sale_at_utc
        ,sku_box_locations.marked_destroyed_at_utc
        ,sku_box_locations.delivered_at_utc
        ,sku_box_locations.moved_to_picking_at_utc
    from sku_box_locations
        left join sku_vendor on sku_box_locations.owner_id = sku_vendor.sku_vendor_id

        /*** Get various join keys to be able to grab information at the time of snapshot date ****/
        left join fc on sku_box_locations.fc_id = fc.fc_id
            and snapshot_date >= fc.adjusted_dbt_valid_from
            and snapshot_date < fc.adjusted_dbt_valid_to
        left join sku on sku_box_locations.sku_id = sku.sku_id
            and snapshot_date >= sku.adjusted_dbt_valid_from
            and snapshot_date < sku.adjusted_dbt_valid_to
        left join lot on sku_box_locations.lot_id = lot.lot_id
            and snapshot_date >= lot.adjusted_dbt_valid_from
            and snapshot_date < lot.adjusted_dbt_valid_to
        
)

select * from inventory_joins

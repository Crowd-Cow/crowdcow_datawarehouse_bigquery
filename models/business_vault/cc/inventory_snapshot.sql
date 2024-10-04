{{
    config(
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

/*** Only starting with dates after 2021-10-28 since that is when we started fully snapshotting the `sku_boxes` data in the new model ***/
dates as ( select calendar_date from {{ ref('stg_reference__date_spine') }} where calendar_date >= '2021-10-28' and calendar_date < current_date() + interval 1 day )
,sku_box as ( select * from {{ ref('stg_cc__sku_boxes') }} )
,fc_location as ( select * from {{ ref('stg_cc__fc_locations') }} )
,fc as ( select * from {{ ref('fcs') }} )
,sku as ( select * from {{ ref('skus') }} )
,lot as ( select * from {{ ref('lots') }} where dbt_valid_to is null )
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,receivable as ( select * from {{ ref('stg_cc__pipeline_receivables') }} )
,pipeline_order as ( select * from {{ ref('stg_cc__pipeline_orders') }} )
,fbq_item as ( select distinct sku_id from {{ ref('int_bid_item_skus') }} where is_fbq_item )
,sku_reservations as (select * from {{ ref('sku_reservations') }} where dbt_valid_to is null )

,inventory_snapshot as (
    select * from (
    select
        sku_box_id
        ,sku_box_key
        ,fc_id
        ,sku_id
        ,owner_id
        ,lot_id
        {# ,pallet_id #}
        ,sku_box_name
        ,min_weight
        ,max_weight
        ,quantity
        ,quantity_reserved
        ,quantity - quantity_reserved as quantity_available
        ,quantity_quarantined
        ,fc_location_parent_id as fc_location_id
        ,created_at_utc
        ,updated_at_utc
        ,marked_not_for_sale_at_utc
        ,marked_destroyed_at_utc
        ,delivered_at_utc
        ,moved_to_picking_at_utc
        ,best_by_date
        ,pack_date
        ,cast(dbt_valid_from as date) as dbt_valid_from
        ,coalesce(cast(dbt_valid_to as date),current_date() + interval 1 day) as dbt_valid_to
        ,row_number() over(partition by sku_box_id, cast(dbt_valid_from as date) order by dbt_valid_from desc) as rn
    from sku_box
    )
    where rn = 1
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
        {# ,inventory_snapshot.pallet_id #}
        ,inventory_snapshot.sku_box_name
        ,inventory_snapshot.min_weight
        ,inventory_snapshot.max_weight
        ,inventory_snapshot.quantity
        ,inventory_snapshot.quantity_reserved
        ,inventory_snapshot.quantity - quantity_reserved as quantity_available
        ,inventory_snapshot.quantity_quarantined
        ,inventory_snapshot.fc_location_id
        ,inventory_snapshot.marked_destroyed_at_utc is not null as is_destroyed
        ,inventory_snapshot.created_at_utc
        ,inventory_snapshot.updated_at_utc
        ,inventory_snapshot.marked_not_for_sale_at_utc
        ,inventory_snapshot.marked_destroyed_at_utc
        ,inventory_snapshot.delivered_at_utc
        ,inventory_snapshot.moved_to_picking_at_utc
        ,inventory_snapshot.best_by_date
        ,inventory_snapshot.pack_date
    from dates
        left join inventory_snapshot on dates.calendar_date >= inventory_snapshot.dbt_valid_from
            and dates.calendar_date < inventory_snapshot.dbt_valid_to
)

,sku_box_locations as (
    select 
        daily_sku_boxes.*
        ,fc_location.location_type
        ,fc_location.location_name
        
        ,coalesce(case 
            when fc_location.is_sellable then daily_sku_boxes.quantity_available
            else 0 
         end,0) as quantity_sellable
        
        ,ifnull(fc_location.is_sellable,FALSE) as is_sellable
    from daily_sku_boxes
        left join fc_location on daily_sku_boxes.fc_location_id = fc_location.fc_location_id
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

,sku_reservations_aggregation as (
    select
        sku_id
        ,fc_id
        ,sum(sku_reservation_quantity) as sku_reservation_quantity
    from sku_reservations 
    group by 1,2

)
,inventory_joins as (
    select 
        {{ dbt_utils.surrogate_key(['sku_box_locations.snapshot_date','sku_box_locations.sku_box_key'] ) }} as inventory_snapshot_id
        ,sku_box_locations.*
        ,fc.fc_key
        ,sku.sku_key
        ,lot.lot_number
        ,sku_vendor.sku_vendor_name as sku_box_owner_name
        ,sku.sku_price_usd
        --,sku.sku_cost_usd

        ,case
            when sku_vendor.is_marketplace and nullif(get_lot_cost_per_unit.cost_per_unit_usd,0) is null then coalesce(nullif(sku.marketplace_cost_usd,0),sku.owned_sku_cost_usd)
            when not sku_vendor.is_marketplace and nullif(get_lot_cost_per_unit.cost_per_unit_usd,0) is null then sku.owned_sku_cost_usd
            else get_lot_cost_per_unit.cost_per_unit_usd
         end as sku_cost_usd

        ,sku_vendor.is_marketplace
        ,sku_vendor.is_rastellis
        ,fbq_item.sku_id is not null as is_configured_for_fbq
        ,lot.delivered_at_utc as lot_delivered_at_utc
        ,sku_reservations_aggregation.sku_reservation_quantity /** As this is reservation qqt is not box level there is the need to select the right aggregation in Looker**/
    from sku_box_locations
        left join sku_vendor on sku_box_locations.owner_id = sku_vendor.sku_vendor_id
        left join lot on sku_box_locations.lot_id = lot.lot_id
        left join get_lot_cost_per_unit on lot.lot_number = get_lot_cost_per_unit.lot_number
            and get_lot_cost_per_unit.sku_id = sku_box_locations.sku_id
        left join fbq_item on sku_box_locations.sku_id = fbq_item.sku_id

        /*** Get various join keys to be able to grab information at the time of snapshot date ****/
        left join fc on sku_box_locations.fc_id = fc.fc_id
                    and sku_box_locations.snapshot_date >= cast(fc.adjusted_dbt_valid_from as date)
                    and sku_box_locations.snapshot_date < cast(fc.adjusted_dbt_valid_to as date)
        left join sku on sku_box_locations.sku_id = sku.sku_id
                  and sku_box_locations.snapshot_date >= cast(sku.adjusted_dbt_valid_from as date)
                  and sku_box_locations.snapshot_date < cast(sku.adjusted_dbt_valid_to as date)
        left join sku_reservations_aggregation on sku_box_locations.sku_id = sku_reservations_aggregation.sku_id
            and sku_box_locations.fc_id = sku_reservations_aggregation.fc_id
        
)

,add_sku_metrics as (
    select
        inventory_snapshot_id
        ,snapshot_date
        ,sku_box_id
        ,sku_box_key
        ,fc_id
        ,fc_key
        ,sku_id
        ,sku_key
        ,owner_id
        ,lot_number
        {# ,pallet_id #}
        ,fc_location_id
        ,sku_box_name
        ,sku_box_owner_name
        ,location_type
        ,location_name
        ,min_weight
        ,max_weight
        ,quantity
        ,quantity * sku_price_usd as potential_revenue
        ,quantity * sku_cost_usd as sku_cost
        /**
        ,quantity_reserved
        ,quantity_reserved * sku_price_usd as potential_revenue_reserved
        ,quantity_reserved * sku_cost_usd as sku_cost_reserved **/
        ,sku_reservation_quantity as quantity_reserved
        ,sku_reservation_quantity * sku_price_usd as potential_revenue_reserved
        ,sku_reservation_quantity * sku_cost_usd as sku_cost_reserved  
        ,quantity_available
        ,quantity_available * sku_price_usd as potential_revenue_available
        ,quantity_available * sku_cost_usd as sku_cost_available
        ,quantity_quarantined
        ,quantity_quarantined * sku_price_usd as potential_revenue_quarantined
        ,quantity_quarantined * sku_cost_usd as sku_cost_quarantined
        ,quantity_sellable
        ,quantity_sellable * sku_price_usd as potential_revenue_sellable
        ,quantity_sellable * sku_cost_usd as sku_cost_sellable
        ,snapshot_date - cast(lot_delivered_at_utc as date) as days_from_delivery
        ,is_sellable
        ,is_destroyed
        ,is_marketplace
        ,is_rastellis
        ,is_configured_for_fbq
        ,created_at_utc
        ,updated_at_utc
        ,marked_not_for_sale_at_utc
        ,marked_destroyed_at_utc
        ,delivered_at_utc
        ,moved_to_picking_at_utc
        ,lot_delivered_at_utc
        ,best_by_date
        ,pack_date
        ,coalesce(best_by_date,date_add(pack_date, interval 365 day)) as proxy_bbd
    from inventory_joins
)

select * from add_sku_metrics

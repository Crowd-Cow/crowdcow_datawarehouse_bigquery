with sku_box as ( select * from {{ ref('stg_cc__sku_boxes') }} )

,inventory_snapshot as (
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
    from sku_box
    qualify row_number() over(partition by sku_box_id, cast(dbt_valid_from as date) order by dbt_valid_from desc) = 1
    )
select * from inventory_snapshot where quantity > 0
    
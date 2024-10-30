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
        ,COALESCE(CAST(dbt_valid_to AS DATE), DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)) AS dbt_valid_to
        ,row_number() over(partition by sku_box_id, cast(dbt_valid_from as date) order by dbt_valid_from desc) as rn
    from sku_box
    where quantity > 0

    )
select * from inventory_snapshot where rn = 1 
    
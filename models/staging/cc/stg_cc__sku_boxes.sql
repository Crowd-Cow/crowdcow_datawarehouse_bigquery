with source as (

    select * from {{ ref('sku_boxes_ss') }}

),

renamed as (

    select
        id as sku_box_id
        ,dbt_scd_id as sku_box_key
        ,fc_id as fulfillment_center_id
        ,sku_id
        ,marked_destroyed_at as marked_destroyed_at_utc
        ,quantity
        ,max_weight
        ,quarantined_quantity
        ,created_at as created_at_utc
        ,{{ clean_strings('name') }} as sku_box_name
        ,fc_location_parent_id
        ,marked_not_for_sale_at as marked_not_for_sale_at_utc
        ,updated_at as updated_at_utc
        ,quantity_reserved
        ,barcode
        ,delivered_at as delivered_at_utc
        ,pallet_id
        ,min_weight
        ,moved_to_picking_at as moved_to_picking_at_utc
        ,owner_id
        ,lot_id
        ,manually_queued_for_on_deck as is_manually_queued_for_on_deck
        ,manually_queued_for_picking as is_manually_queued_for_picking
        ,scanned as is_scanned
        ,filled as is_filled
        ,printed as is_printed
        ,dbt_valid_to
        ,dbt_valid_from

    from source

)

select * from renamed


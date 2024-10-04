with

locations as ( select * from {{ source('cc', 'fc_locations') }} where __deleted is null )

,renamed as (
    select
        id as fc_location_id
        ,updated_at as updated_at_utc
        ,sad_cow_bin_id
        ,{{ clean_strings('name') }} as location_name
        ,parent_id as fc_location_parent_id
        --, clean_strings('merchandising_request_status') }} as merchandising_request_status
        --, clean_strings('fc_transfer_status') }} as fc_transfer_status
        ,on_deck_score
        ,position
        ,created_at as created_at_utc
        ,fc_id
        ,barcode
        ,picking_priority
        ,{{ clean_strings('location_type') }} as location_type
        ,marked_destroyed_at as marked_destroyed_at_utc
        ,sku_box_id
        ,if(sellable = 1, true, false) as is_sellable
        ,on_deck as is_on_deck
    from locations
)

select * from renamed

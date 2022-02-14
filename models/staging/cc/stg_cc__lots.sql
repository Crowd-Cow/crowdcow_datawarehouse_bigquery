with

lot as ( select * from {{ source('cc', 'lots') }} where not _fivetran_deleted )

,renamed as (
    select
        id as lot_id
        ,{{ cents_to_usd('cost_in_cents') }} as cost_usd
        ,sku_vendor_id
        ,yield_weight_in_pounds
        ,updated_at as updated_at_utc
        ,archived_at as archived_at_utc
        ,name as lot_number
        ,fc_id
        ,cows_available_updated_at as cows_available_updated_at_utc
        ,pipeline_order_id
        ,grind_weight_in_pounds
        ,{{ clean_strings('received_but_not_delivered_location_name') }} as received_but_not_delivered_location_name
        ,received_not_delivered_location_id
        ,total_weight_in_pounds
        ,delivered_at as delivered_at_utc
        ,created_at as created_at_utc
        ,owner_id
    from lot
)

select * from renamed

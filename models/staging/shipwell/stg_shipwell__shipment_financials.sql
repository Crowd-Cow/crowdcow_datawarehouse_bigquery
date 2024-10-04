with

source as ( select * from {{ source('shipwell', 'shipments') }} )

,renamed as (
    select
        id as shipment_id
        ,shipment_financial.value:id::text as shipment_financial_id
        ,shipment_financial.value:effective_amount::float as shipment_amount
        ,{{ clean_strings('shipment_financial.value:category::text') }} as shipment_category
        ,{{ clean_strings('shipment_financial.value:unit_name::text') }} as shipment_category_name
        ,shipment_financial.value:created_at::timestamp as created_at_utc
        ,shipment_financial.value:updated_at::timestamp as updated_at_utc
    from source,
        lateral flatten(input => relationship_to_vendor, path => 'vendor_charge_line_items') shipment_financial
)

select * from renamed

with

source as ( select * from {{ source('shipwell', 'shipwell_shipments') }} )

,renamed as (
    select
        id as shipment_id
        ,reference_id
        ,CAST(JSON_EXTRACT_SCALAR(current_carrier, '$.name') AS STRING) AS current_carrier_name
        ,{{ clean_strings('state') }} as shipment_status
        ,total_miles
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
    from source
)

select * from renamed



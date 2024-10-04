with

source as ( select * from {{ source('shipwell', 'shipments') }} )

,renamed as (
    select
        id as shipment_id
        ,tag.value::text as tag_id
    from source,
        lateral flatten( input => metadata, path => 'tags' ) tag
)

select * from renamed

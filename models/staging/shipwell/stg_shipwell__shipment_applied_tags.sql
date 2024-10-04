with

source as ( select * from {{ source('shipwell', 'shipwell_shipments') }} )

,renamed as (
    SELECT 
        source.id AS shipment_id,
        CAST(JSON_EXTRACT_SCALAR(tag, '$') AS STRING) AS tag_id
    FROM 
        source,
        UNNEST(JSON_EXTRACT_ARRAY(source.metadata.tags)) AS tag
)

select * from renamed

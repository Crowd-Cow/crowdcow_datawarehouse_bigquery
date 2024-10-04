with

source as ( select * from {{ source('shipwell', 'shipwell_shipments') }} )

,renamed as (
SELECT
    source.id AS shipment_id,
    CAST(JSON_EXTRACT_SCALAR(shipment_financial, '$.id') AS STRING) AS shipment_financial_id,
    CAST(JSON_EXTRACT_SCALAR(shipment_financial, '$.effective_amount') AS FLOAT64) AS shipment_amount,
    {{ clean_strings('CAST(JSON_EXTRACT_SCALAR(shipment_financial, \'$.category\') AS STRING)') }} AS shipment_category,
    {{ clean_strings('CAST(JSON_EXTRACT_SCALAR(shipment_financial, \'$.unit_name\') AS STRING)') }} AS shipment_category_name,
    CAST(JSON_EXTRACT_SCALAR(shipment_financial, '$.created_at') AS TIMESTAMP) AS created_at_utc,
    CAST(JSON_EXTRACT_SCALAR(shipment_financial, '$.updated_at') AS TIMESTAMP) AS updated_at_utc
FROM 
    source,
    UNNEST(JSON_EXTRACT_ARRAY(source.relationship_to_vendor.vendor_charge_line_items)) AS shipment_financial
)

select * from renamed

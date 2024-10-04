with

source as ( select * from {{ source('shipwell', 'purchase_orders') }} )

,renamed as (
    select 
        id as purchase_order_id
        ,CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.id') AS string) AS line_item_id
        ,CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.package_type') AS string) as package_type
        ,CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.package_weight') AS FLOAT64) as item_package_weight
        ,CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.total_packages') AS INT64) as item_total_packages
        ,CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.total_line_item_weight') AS FLOAT64) as item_total_weight
        ,CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.created_at') AS timestamp) as item_created_at_utc
        ,CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.updated_at') AS timestamp) as item_updated_at_utc
    from source,
        UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS flattened_element
)

select * from renamed
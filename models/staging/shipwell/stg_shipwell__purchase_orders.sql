with

source as ( select * from {{ source('shipwell', 'purchase_orders') }} )

,renamed as (
    select 
        id as purchase_order_id
        
        ,REGEXP_EXTRACT(
        TRIM(REGEXP_REPLACE(purchase_order_number, r'\s*-*[A-Za-z]*', '')),
        r'^[0-9]{4}$'
        )AS lot_number
        ,shipment_id
        ,{{ clean_strings('overall_status') }} as overall_status
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
    from source
)

select * from renamed

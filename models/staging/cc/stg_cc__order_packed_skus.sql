with source as (

    select * from {{ source('cc', 'order_packed_skus') }} 

),

renamed as (

    select
        id as order_packed_sku_id
        --, clean_strings('name') }} as order_packed_sku_name
        ,{{cents_to_usd('payment_processing_fee_in_cents') }} as payment_processing_fee_usd
        ,order_id
        ,sku_id
        ,created_at as created_at_utc
        ,sku_reservation_id
        --,barcode as sku_barcode
        --,farm_id
        ,acumatica_confirmed_at as acumatica_confirmed_at_utc
        ,quantity as sku_quantity
        ,sku_box_id
        ,{{ cents_to_usd('platform_fee_in_cents') }} as platform_fee_usd
        ,{{ cents_to_usd('fulfillment_fee_in_cents') }} as fulfillment_fee_usd
        ,updated_at as updated_at_utc
        --, clean_strings('reason') }} as reason
        --, clean_strings('farm_name') }} as farm_name
        --,user_id
        ,committed_at as committed_at_utc
        ,{{ cents_to_usd('cost_in_cents') }} as sku_cost_usd
       --,weight as sku_weight

    from source

)

select * from renamed


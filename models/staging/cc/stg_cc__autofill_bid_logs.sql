with source as (

    select * from {{ source('cc', 'autofill_bid_logs') }}

),

renamed as (

    select
        id as autofill_bid_log_id
        ,autofill_order_log_id
        ,created_at as created_at_utc
        ,quantity as autofill_quantity
        ,updated_at as updated_at_utc
        ,product_permutation_id as autofill_product_permutation_id
        ,{{ clean_strings('reason') }} as reason
        ,{{ clean_strings('target_sku') }} as target_sku_name
        ,bid_id
        ,target_quantity
        ,sku_id as autofill_sku_id
        ,{{ clean_strings('name') }} as autofill_sku_name
        ,target_sku_id
        ,target_product_permutation_id

    from source

)

select * from renamed


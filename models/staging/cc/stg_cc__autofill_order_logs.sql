with source as (

    select * from {{ source('cc', 'autofill_order_logs') }} 

),

renamed as (

    select
        id as autofill_order_log_id
        ,updated_at as updated_at_utc
        ,{{ cents_to_usd('previous_order_subtotal_cents') }} as previous_order_subtotal_usd
        ,{{ cents_to_usd('final_subtotal_cents') }} as final_subtotal_usd
        ,order_id
        ,created_at as created_at_utc
        ,previous_order_id
        ,{{ cents_to_usd('target_subtotal_cents') }} as target_subtotal_usd
        ,{{ clean_strings('notes') }} as notes
        ,{{ clean_strings('autofill_type') }} as autofill_type
        ,{{ cents_to_usd('initial_subtotal_cents') }} as initial_subtotal_usd
        ,filled_at as filled_at_date
    from source

)

select * from renamed


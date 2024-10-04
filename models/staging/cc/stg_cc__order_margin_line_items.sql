{{
    config(
        enabled=false
    )
}}
with source as (

    select * from {{ source('cc', 'order_margin_line_items') }} 

),

renamed as (

    select
        id as order_margin_id
        ,{{ clean_strings('notes') }} as order_margin_notes
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,cost_history_id
        ,sku_id
        ,order_id
        ,{{ clean_strings('name') }} as order_margin_name
        ,{{ clean_strings('category') }} as order_margin_category
        ,fc_id
        ,{{ cents_to_usd('amount_in_cents') }} as order_margin_amount_usd
        ,bid_id

    from source

)

select * from renamed


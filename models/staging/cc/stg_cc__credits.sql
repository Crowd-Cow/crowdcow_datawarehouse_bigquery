with source as (

    select * from {{ source('cc', 'credits') }} where not _fivetran_deleted

),

renamed as (

    select
        id as credit_id
        ,promotion_id
        ,bid_item_id
        ,cow_cash_entry_source_id
        ,{{ cents_to_usd('discount_in_cents') }} as credit_discount_usd
        ,created_at as created_at_utc
        ,user_id
        ,{{ clean_strings('credit_type') }} as credit_type
        ,order_id
        ,bid_id
        ,updated_at as updated_at_utc
        ,{{ clean_strings('description') }} as credit_description
        ,{{ convert_percent('discount_percent') }} as discount_percent
        ,hide_from_user as is_hidden_from_user
        ,controlled_by_promotion as is_controlled_by_promotion
    from source

)

select * from renamed


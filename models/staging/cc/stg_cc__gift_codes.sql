with source as (

    select * from {{ source('cc', 'gift_codes') }} where not _fivetran_deleted

),

renamed as (

    select
        id as gift_code_id
        ,subscription_id
        ,{{ convert_percent('discount_percent') }} as discount_percent
        ,order_id
        ,{{ clean_strings('campaign_type') }} as campaign_type
        ,{{ clean_strings('campaign_name') }} as campaign_name
        ,{{ cents_to_usd('minimum_order_amount_in_cents') }} as minimum_order_amount_usd
        ,token as gift_code_token
        ,{{ cents_to_usd('discount_in_cents') }} as discount_amount_usd
        ,bid_item_id
        ,user_id
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,partner_id
        ,redeemed_at as redeemed_at_utc

    from source

)

select * from renamed


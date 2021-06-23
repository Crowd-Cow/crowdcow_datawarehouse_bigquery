
with source as (

    select * from {{ source('cc', 'product_variants') }}

),

renamed as (

    select
      id as product_variant_id
      , {{ cents_to_usd('min_product_bundle_value_in_cents') }} as min_product_bundle_value_usd
      , fc_id 
      , {{ clean_strings('highlight_text') }} as product_variant_highlight_text
      , strike_through_percent_off
      , {{ clean_strings('description') }} as description
      , created_at as created_at_utc
      , {{ cents_to_usd('max_product_bundle_value_in_cents') }} as max_product_bundle_value_usd
      , {{ clean_strings('automated_highlight_text_type') }} as product_variant_automated_highlight_text_type
      , {{ cents_to_usd('price_in_cents') }} as price_usd
      , product_id
      , updated_at as updated_at_utc
      , image_url as product_variant_image_url
      , can_subscribe_to_variant 
      , always_available as is_product_variant_always_available

    from source

)

select * from renamed

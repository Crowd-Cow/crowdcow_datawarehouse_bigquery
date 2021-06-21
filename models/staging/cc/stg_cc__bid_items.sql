{{
  config(
    tags=["stage"]
  )
}}

with source as (

  select * from  {{ source('cc', 'bid_items') }} as b

)

, renamed as ( 

  select 
    created_at as created_at_utc
    , {{ clean_string('description') }} as description
    , {{ clean_string('description_template') }} as description_template
    , event_id
    , hide_from_user as is_hidden_from_user
    , id
    , {{ clean_string('item_photo_url') }} as item_photo_url
    , {{ cents_to_usd('item_price_cents') }} as item_price_usd
    , {{ clean_string('item_type') }} as item_type
    , {{ clean_string('name') }} as name
    , {{ clean_string('portion_type') }} as portion_type 
    , quantity_available
    , {{ cents_to_usd('strike_through_price_cents') }} as strike_through_price_usd
    , {{ clean_string('subtype') }} as subtype
    , {{ clean_string('token') }} as token
    , updated_at as updated_at_utc
    , product_variant_id
    , max_dynamic_quantity  
    , {{ clean_string('highlight_text') }} as highlight_text
    , stackable as is_stackable
    , new_customers_only as is_new_customers_only
    , bid_item_group_id
    , limit_one_per_order as limit_one_per_order
    , valid_price as is_valid_price
    , {{ clean_string('skus_with_quantities_hash') }} as skus_with_quantities_hash
    , product_permutation_id
    , automated_highlight_text_type
    , always_available as is_always_available

    from source as s

)

select * from renamed


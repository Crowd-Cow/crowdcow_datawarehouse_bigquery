{{
  config(
    tags=["stage"]
  )
}}

with source as (

  select * from  {{ source('cc', 'bids') }} 

),

renamed as ( 

  select
    id   as bid_id
    , bid_item_id
    , created_at as created_at_utc
    , from_sms_log_entry_id
    , from_user_mail_log_entry_id
    , {{ cents_to_usd('item_price_cents') }} as item_price_usd
    , mix_portion_group_ids
    , order_id
    , bid_quantity
    , updated_at as updated_at_utc
    , user_id
    , product_id
    , promotion_id
    , {{ clean_strings('name')  }} as product_name
    , {{ clean_strings('description') }} as product_description
    , {{ clean_strings('item_photo_url') }} as item_photo_url
    , subscription_id
    , custom_subscription_item_id
    , {{ clean_strings('token') }} as bid_token
    , {{ clean_strings('reason') }} as autofill_reason
    , target_sku_id
    , fill_score
    , reserve_inventory_immediately as is_reserve_inventory_immediately
    , {{ clean_strings('fill_type') }} as fill_type
    , fulfillment_at_risk as is_fulfillment_at_risk
    , product_permutation_id
    , target_product_permutation_id
    , first_stuck_at as first_stuck_at_utc

    from source 

)

select * from renamed


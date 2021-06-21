{{
  config(
    tags=["stage"]
  )
}}

with source as (

  select * from  {{ source('cc', 'bids') }} as b

)

, renamed as ( 

  select
    s.id   as bid_id
    , s.bid_item_id
    , s.created_at as created_at_utc
    , s.from_sms_log_entry_id
    , s.from_user_mail_log_entry_id
    , {{ cents_to_usd('s.item_price_cents') }} as item_price_usd
    , s.mix_portion_group_ids
    , s.order_id
    , s.quantity
    , s.updated_at as updated_at_utc
    , s.user_id
    , s.product_id
    , s.promotion_id
    , s.name 
    , s.description
    , s.item_photo_url
    , s.subscription_id
    , s.custom_subscription_item_id
    , s.token
    , s.reason
    , s.target_sku_id
    , s.fill_score
    , s.reserve_inventory_immediately
    , s.fill_type
    , s.fulfillment_at_risk
    , s.product_permutation_id
    , s.target_product_permutation_id
    , s.first_stuck_at as first_stuck_at_utc

    from source as s

)

select * from renamed


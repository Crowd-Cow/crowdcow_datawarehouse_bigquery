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
    b.id   as bid_id
    , b.bid_item_id
    , b.created_at as created_at_utc
    , b.from_sms_log_entry_id
    , b.from_user_mail_log_entry_id
    , {{ cents_to_usd('b.item_price_cents') }} as item_price_usd
    , b.mix_portion_group_ids
    , b.order_id
    , b.quantity
    , b.updated_at as updated_at_utc
    , b.user_id
    , b.product_id
    , b.promotion_id
    , b.name as event_name
    , b.description
    , b.item_photo_url
    , b.subscription_id
    , b.custom_subscription_item_id
    , b.token
    , b.reason
    , b.target_sku_id
    , b.fill_score
    , b.reserve_inventory_immediately
    , b.fill_type
    , b.fulfillment_at_risk
    , b.product_permutation_id
    , b.target_product_permutation_id
    , b.first_stuck_at as first_stuck_at_utc

    from source
    
)

select * from renamed


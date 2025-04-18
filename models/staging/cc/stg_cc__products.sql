
with source as (

  select * from  {{ ref('products_ss') }}  where (_fivetran_deleted is null or _fivetran_deleted = false)

),

renamed as ( 

  select
   id as product_id
  ,dbt_scd_id as product_key
  ,{{ clean_strings('title') }} as product_title
  ,{{ clean_strings('description') }} as product_description
  ,published_at as published_at_utc
  ,{{ clean_strings('seo_page_title') }} as seo_page_title
  ,{{ clean_strings('seo_meta_description') }} as seo_meta_description
  ,{{ clean_strings('url_handle') }} as url_handle
  ,created_at as created_at_utc
  ,updated_at as updated_at_utc
  ,canonical_product_id
  ,stackable as is_stackable
  ,{{ clean_strings('subtype') }} as product_subtype
  ,token as product_token
  ,archived_at as archived_at_utc
  ,{{ cents_to_usd('price_in_cents') }} as price_in_usd
  ,multifarm as is_multifarm
  ,availability_alert_threshold
  ,{{ clean_strings('highlight_text') }} as product_highlight_text
  ,{{ convert_percent('strike_through_percent_off') }} strike_through_percent_off
  ,new_customers_only as is_new_customers_only
  ,limit_one_per_order as is_limit_one_per_order
  ,{{ cents_to_usd('min_product_bundle_value_in_cents') }} as min_product_bundle_value_in_usd 
  ,{{ cents_to_usd('max_product_bundle_value_in_cents') }} as max_product_bundle_value_in_usd
  ,fc_id
  ,{{ clean_strings('internal_note') }} as product_internal_note
  --,loyalty_reward_order_number
  --,loyalty_reward_apply_window_days
  --,loyalty_reward_daily_rate
  --, clean_strings('loyalty_reward_message') }} as loyalty_reward_message
  ,{{ clean_strings('automated_highlight_text_type') }} as automated_highlight_text_type
  ,always_available as is_always_available
  ,alacarte as is_alacarte
  ,dbt_valid_to
  ,dbt_valid_from
  ,case
      when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
      else dbt_valid_from
    end as adjusted_dbt_valid_from
  ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source 

)

select * from renamed


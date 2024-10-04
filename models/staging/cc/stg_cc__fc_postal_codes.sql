with

source as ( select * from {{ source('cc', 'fc_postal_codes') }} where not _fivetran_deleted )

,renamed as (
  select
    id as fc_postal_code_id
    ,created_at as created_at_utc
    ,fc_id
    ,transit_days_delay
    ,transit_method
    ,transit_days
    ,updated_at as updated_at_utc
    ,postal_code
    ,priority
    ,add_additional_ice_in_lbs
    ,delay_delivery_until
    ,{{ cents_to_usd('override_shipping_fee_cents') }} as override_shipping_fee_usd
    ,sunday_delivery as has_sunday_delivery
    ,saturday_delivery as has_saturday_delivery
    ,{{ cents_to_usd('free_shipping_threshold_cents') }} as free_shipping_threshold_usd
  from source
)

select * from renamed

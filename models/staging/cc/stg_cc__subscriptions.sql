with source as (

    select * from {{ source('cc', 'subscriptions') }} where not _fivetran_deleted

),

renamed as (
--TODO: Rename subscription -> membership and fix downstream
    select
        id as subscription_id
        ,created_at as subscription_created_at_utc
        ,{{ clean_strings('subscription_type') }} as subscription_type
        ,{{ clean_strings('delivery_name') }} as delivery_name
        ,{{ clean_strings('delivery_street_address_1') }} as delivery_street_address_1
        ,{{ clean_strings('delivery_street_address_2') }} as delivery_street_address_2
        ,{{ clean_strings('delivery_city') }} as delivery_city
        ,{{ clean_strings('delivery_state') }} as delivery_state
        ,{{ clean_strings('delivery_postal_code') }} as delivery_postal_code
        ,reward_frequency as subscription_reward_frequency
        ,cancelled_at as subscription_cancelled_at_utc
        ,user_id
        ,token as subscription_token
        ,{{ cents_to_usd('price_in_cents') }} as subscription_price_usd
        ,{{ clean_strings('cancelled_reason') }} as subscription_cancelled_reason
        ,{{ clean_strings('signup_state') }} as subscription_signup_status
        ,updated_at as updated_at_utc
        ,{{ clean_strings('farm_category') }} as farm_category
        ,product_id
        ,estimated_fulfillment_date
        ,{{ clean_strings('renew_period_type') }} as subscription_renew_period_type
        ,{{ clean_strings('stripe_card_id') }} as stripe_card_id
        ,phone_number_id
        ,renews_at as subscription_renews_at_utc
        ,active as is_uncancelled_membership

    from source

)

select * from renamed


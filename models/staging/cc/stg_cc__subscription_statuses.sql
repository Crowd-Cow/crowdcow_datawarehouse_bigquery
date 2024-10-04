with

source as ( select * from {{ source('cc', 'subscription_statuses') }} )

,renamed as (
    select
        order_missing as is_order_missing
        ,week_out_notification_sent as was_week_out_notification_sent
        ,created_at as created_at_utc
        ,order_id
        ,updated_at as updated_at_utc
        ,all_inventory_reserved as is_all_inventory_reserved
        ,order_cancelled as is_order_cancelled
        ,arrival_date as arrival_date_utc
        ,needs_customer_confirmation as does_need_customer_confirmation
        ,bids_fulfillment_at_risk as is_bids_fulfillment_at_risk
        ,time_to_charge as is_time_to_charge
        ,payment_failure as is_payment_failure
        ,referred_to_customer_service as was_referred_to_customer_service
        ,invalid_postal_code as is_invalid_postal_code
        ,order_charged as is_order_charged
        ,can_retry_payment
        ,id as subscription_status_id
        ,{{ cents_to_usd('order_total_cents') }} as order_total_usd
        ,{{ cents_to_usd('order_subtotal_cents') }} as order_subtotal_usd
        ,fulfillment_date as fulfillment_at_utc
        ,under_order_minimum as is_under_order_minimum
        ,order_scheduled_in_past as is_order_scheduled_in_past
        ,subscription_id
    from source
)

select * from renamed

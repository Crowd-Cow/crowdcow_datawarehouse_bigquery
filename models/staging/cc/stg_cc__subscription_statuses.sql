with

source as ( select * from {{ source('cc', 'subscription_statuses') }} )

,renamed as (
    select
        if(order_missing = 1, true, false) as is_order_missing 
        ,if(week_out_notification_sent = 1 ,true,false) as was_week_out_notification_sent
        ,created_at as created_at_utc
        ,order_id
        ,updated_at as updated_at_utc
        ,if(all_inventory_reserved = 1, true, false) as is_all_inventory_reserved 
        ,if(order_cancelled = 1, true, false) as is_order_cancelled 
        ,arrival_date as arrival_date_utc
        ,if(needs_customer_confirmation = 1 ,true,false) as does_need_customer_confirmation
        ,if(bids_fulfillment_at_risk = 1, true, false) as is_bids_fulfillment_at_risk 
        ,if(time_to_charge = 1, true, false) as is_time_to_charge 
        ,if(payment_failure = 1, true, false) as is_payment_failure 
        ,if(referred_to_customer_service = 1 ,true,false) as was_referred_to_customer_service
        ,if(invalid_postal_code = 1, true, false) as is_invalid_postal_code 
        ,if(order_charged = 1, true, false) as is_order_charged 
        ,if(can_retry_payment = 1 ,true,false) as can_retry_payment
        ,id as subscription_status_id
        ,{{ cents_to_usd('order_total_cents') }} as order_total_usd
        ,{{ cents_to_usd('order_subtotal_cents') }} as order_subtotal_usd
        ,fulfillment_date as fulfillment_at_utc
        ,if(under_order_minimum = 1, true, false) as is_under_order_minimum 
        ,if(order_scheduled_in_past = 1, true, false) as is_order_scheduled_in_past 
        ,subscription_id
    from source
)

select * from renamed

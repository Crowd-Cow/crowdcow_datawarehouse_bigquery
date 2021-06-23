with source as (

  select * from {{ source('cc', 'orders') }}

),

renamed as (

  select
     id as order_id
    ,parent_order_id as parent_order_id
    ,campaign_id
    ,subscription_id
    ,created_by_subscription_id as order_created_by_subscription_id
    ,cancelled_by_user_id as order_cancelled_by_user_id
    ,fc_id
    ,ahoy_visit_id
    ,user_id
    ,packing_wave_id
    ,gift_recipient_id
    ,phone_number_id
    ,packer_id
    ,bid_id
    ,event_id as order_event_id -- As found in cc.events, not an Ahoy Event
    ,{{ clean_strings('session_id') }} as order_session_id
    ,created_by_user_id as order_created_by_user_id
    ,coolant_weight_in_pounds as order_coolant_weight_in_pounds
    ,{{ clean_strings('portion_errors') }} as order_portion_errors
    ,order_contents_last_notified_hash
    ,{{ clean_strings('packing_notes') }} as order_packing_notes
    ,paid_at as order_paid_at_utc
    ,address_verified_at as order_address_verified_at_utc
    ,portions_fulfilled as order_portions_fulfilled -- Complex string that looks like a dump of Ruby objects
    ,purchase_postage_failed_at as order_purchase_postage_failed_at_utc
    ,additional_coolant_weight_in_pounds as order_additional_coolant_weight_in_pounds
    ,order_contents_last_notified_at as order_contents_last_notified_at_utc
    ,prepick_at as order_prepick_at_utc
    ,updated_address_at as order_updated_address_at_utc
    ,{{ cents_to_usd('handling_fee_cents') }} as order_handling_fee_usd
    ,confirmation_sent_at as order_confirmation_sent_at_utc
    ,{{ cents_to_usd('processing_fee_cents') }} as order_processing_fee_usd
    ,{{ clean_strings('delivery_city') }} as order_delivery_city
    ,{{ clean_strings('original_url') }} as order_original_url
    ,cancelled_at as order_cancelled_at_utc
    ,confirmed_at as order_confirmed_at_utc
    ,first_stuck_at as order_first_stuck_at_utc
    ,print_pick_label_last_attempted_at as order_print_pick_label_last_attempted_at_utc
    ,delivery_locked_at as order_delivery_locked_at_utc
    ,picked_up_at as order_picked_up_at_utc
    ,notes_fulfilled_at as order_notes_fulfilled_at_utc
    ,{{ cents_to_usd('shipping_fee_cents') }} as order_shipping_fee_usd
    ,notes_updated_at as order_notes_updated_at_utc
    ,scheduled_arrival_date as order_scheduled_arrival_date_utc
    ,print_prepick_label_last_attempted_at as order_print_prepick_label_last_attempted_at_utc
    ,{{ clean_strings('force_postage_carrier') }} as order_force_postage_carrier 
    ,scheduled_fulfillment_date as order_scheduled_fulfillment_date_utc
    ,{{ cents_to_usd('total_price_cents') }} as order_total_price_usd
    ,picked_at as order_picked_at_utc
    ,{{ clean_strings('selected_delivery_method') }} as order_selected_delivery_method
    ,{{ clean_strings('override_postage_service') }} as order_override_postage_service
    ,{{ clean_strings('token') }} as order_token
    ,{{ cents_to_usd('credit_total_cents') }} as order_total_discount_usd
    ,day_out_notified_at as order_day_out_notified_at_utc
    ,print_last_attempted_at as order_print_last_attempted_at_utc
    ,marked_eligible_to_prepack_at as order_marked_eligible_to_prepack_at_utc
    ,packing_notes_updated_at as order_packing_notes_updated_at_utc
    ,customer_viewed_at as order_customer_viewed_at_utc
    ,created_at as order_created_at_utc
    ,{{ clean_strings('current_state') }} as order_current_state
    ,delivery_postal_code as order_delivery_postal_code
    ,{{ clean_strings('notes') }} as order_notes
    ,delivery_latitude as order_delivery_latitude
    ,print_handwritten_note_last_attempted_at as order_print_handwritten_note_last_attempted_at_utc
    ,{{ clean_strings('order_identifier') }} as order_identifier
    ,{{ clean_strings('confirmation_token') }} as order_confirmation_token
    ,{{ cents_to_usd('sales_tax_cents') }} as order_sales_tax_usd
    ,{{ clean_strings('override_postage_carrier') }} as order_override_postage_carrier
    ,{{ clean_strings('delivery_name') }} as order_delivery_name
    ,packing_override_at as order_packing_override_at_utc
    ,last_changed_at as order_last_changed_at_utc
    ,checkout_completed_at as order_checkout_completed_at_utc
    ,{{ clean_strings('order_type') }} as order_type
    ,{{ clean_strings('where_order_placed') }} as where_order_placed
    ,shipments_computed_at as order_shipments_computed_at_utc
    ,next_box_notified_at as order_next_box_notified_at_utc
    ,logged_google_analytics_conversion_at as order_logged_google_analytics_conversion_at_utc
    ,{{ clean_strings('delivery_street_address_1') }} as order_delivery_street_address_1
    ,{{ clean_strings('delivery_street_address_2') }} as order_delivery_street_address_2
    ,update_payment_and_shipping_last_attempted_at as order_update_payment_and_shipping_last_attempted_at_utc
    ,updated_at as order_updated_at_utc
    ,{{ clean_strings('scheduled_fulfillment_date_override_note') }} as order_scheduled_fulfillment_date_override_note
    ,bids_count as order_bids_count
    ,{{ clean_strings('postage_label_note') }} as order_postage_label_note
    ,packed_at as order_packed_at_utc
    ,{{ clean_strings('delivery_state') }} as order_delivery_state
    ,total_weight_in_pounds as order_total_weight_in_pounds
    ,delivery_longitude as order_delivery_longitude
    ,updated_payment_info_at as order_updated_payment_info_at_utc
    ,delivery_elevation as order_delivery_elevation
    ,set_aside_at as order_set_at_utc
    ,scheduled_fulfillment_date_override_by_user_id as order_scheduled_fulfillment_date_override_by_user_id
    ,delivery_address_is_residential as order_delivery_address_is_residential
    ,easypost_skip_address_verification as order_easypost_will_skip_address_verification
    ,silence_delivery_notifications as order_should_silence_delivery_notifications
    ,first_order_ever as is_first_order_ever
    ,ignore_weather_alerts as order_should_ignore_weather_alerts
    ,easypost_requires_signature as order_easypost_requires_signature
    ,use_as_default_address as order_use_as_default_address
    ,recurring as is_recurring
    ,earliest_to_charge_at as order_earliest_to_charge_at_utc
    ,stripe_card_id
    ,{{ clean_strings('stripe_card_country') }} as stripe_card_country
    ,stripe_charge_id -- Not converting to uppercase because the difference between upper and lower is significant in this ID (ex. ch_0IvyC774pVzbTfjziW0s9lUS)
    ,{{ clean_strings('stripe_failure_message') }} as stripe_failure_message
    ,stripe_card_exp_year
    ,stripe_charge_attempted_at as stripe_charge_attempted_at_utc
    ,stripe_card_zip
    ,{{ clean_strings('stripe_card_brand') }} as stripe_card_brand
    ,stripe_card_exp_month
    ,stripe_card_id_last_updated_at as stripe_card_id_last_updated_at_utc
    ,stripe_card_fingerprint -- Not converting to uppercase because the difference between upper and lower is significant in this fingerprint (ex. l3wrJ08V0t1SSecS)
    ,{{ clean_strings('stripe_failure_code') }} as stripe_failure_code
    ,{{ clean_strings('stripe_card_funding') }} as stripe_card_funding
    ,stripe_card_last4

  from source

)

select * from renamed

with 
source as ( select * from {{ source('stripe', 'charge') }} )

,renamed as (

    select
        id as stripe_charge_id
        ,connected_account_id
        ,{{ cents_to_usd('amount') }} as charge_amount
        ,{{ cents_to_usd('amount_refunded') }} as amount_refunded
        ,application as charge_application
        ,{{ clean_strings('billing_detail_address_city') }} as billing_detail_address_city
        ,{{ clean_strings('billing_detail_address_country') }} as billing_detail_address_country
        ,{{ clean_strings('billing_detail_address_line_1') }} as billing_detail_address_line_1
        ,{{ clean_strings('billing_detail_address_line_2') }} as billing_detail_address_line_2
        ,{{ clean_strings('billing_detail_address_postal_code') }} as billing_detail_address_postal_code
        ,{{ clean_strings('billing_detail_address_state') }} as billing_detail_address_state
        ,{{ clean_strings('billing_detail_email') }} as billing_detail_email
        ,{{ clean_strings('billing_detail_name') }} as billing_detail_name
        ,{{ clean_strings('billing_detail_phone') }} as billing_detail_phone
        ,{{ cents_to_usd('application_fee_amount') }} as application_fee_amount
        ,{{ clean_strings('calculated_statement_descriptor') }} calculated_statement_descriptor
        ,captured as is_captured
        ,created as created_at_utc
        ,{{ clean_strings('currency') }} as currency
        ,{{ clean_strings('description') }} as charge_description
        ,{{ clean_strings('destination') }} as destination
        ,{{ clean_strings('failure_code') }} as failure_code
        ,{{ clean_strings('failure_message') }} as failure_message
        ,{{ clean_strings('fraud_details_user_report') }} as fraud_details_user_report
        ,{{ clean_strings('fraud_details_stripe_report') }} as fraud_details_stripe_report
        ,livemode as is_livemode
        ,metadata::variant as metadata
        ,{{ clean_strings('on_behalf_of') }} as on_behalf_of
        ,{{ clean_strings('outcome_network_status') }} as outcome_network_status
        ,{{ clean_strings('outcome_reason') }} as outcome_reason
        ,{{ clean_strings('outcome_risk_level') }} as outcome_risk_level
        ,outcome_risk_score
        ,{{ clean_strings('outcome_seller_message') }} as outcome_seller_message
        ,{{ clean_strings('outcome_type') }} as outcome_type
        ,paid as is_paid
        ,{{ clean_strings('receipt_email') }} as receipt_email
        ,receipt_number
        ,{{ clean_strings('receipt_url') }} as receipt_url
        ,refunded as is_refunded
        ,{{ clean_strings('shipping_address_city') }} as shipping_address_city
        ,{{ clean_strings('shipping_address_country') }} as shipping_address_country
        ,{{ clean_strings('shipping_address_line_1') }} as shipping_address_line_1
        ,{{ clean_strings('shipping_address_line_2') }} as shipping_address_line_2
        ,{{ clean_strings('shipping_address_postal_code') }} as shipping_address_postal_code
        ,{{ clean_strings('shipping_address_state') }} as shipping_address_state
        ,{{ clean_strings('shipping_carrier') }} as shipping_carrier
        ,{{ clean_strings('shipping_name') }} as shipping_name
        ,{{ clean_strings('shipping_phone') }} as shipping_phone
        ,{{ clean_strings('shipping_tracking_number') }} as shipping_tracking_number
        ,card_id
        ,bank_account_id
        ,source_id
        ,source_transfer
        ,statement_descriptor
        ,{{ clean_strings('status') }} as charge_status
        ,{{ clean_strings('transfer_data_destination') }} as transfer_data_destination
        ,{{ clean_strings('transfer_group') }} as transfer_group
        ,balance_transaction_id
        ,customer_id
        ,invoice_id
        ,payment_intent_id
        ,payment_method_id
        ,transfer_id
        ,{{ clean_strings('rule_rule') }} as rule_rule
        ,rule_id
        ,{{ clean_strings('rule_action') }} as rule_action
        ,{{ clean_strings('rule_predicate') }} as rule_predicate

    from source

)

select * from renamed

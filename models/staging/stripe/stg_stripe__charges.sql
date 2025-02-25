with 
source as ( select * from {{ source('stripe', 'stripe_charges') }} )

,renamed as (

    select
        id as stripe_charge_id
        ,null as connected_account_id
        ,{{ cents_to_usd('amount') }} as charge_amount
        ,{{ cents_to_usd('amount_refunded') }} as amount_refunded
        ,application as charge_application
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.city')") }} as billing_detail_address_city
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.country')") }} as billing_detail_address_country
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.line_1')") }} as billing_detail_address_line_1
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.line_2')") }} as billing_detail_address_line_2
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.postal_code')") }} as billing_detail_address_postal_code
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.state')") }} as billing_detail_address_state
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.email')") }} as billing_detail_email
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.name')") }} as billing_detail_name
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(billing_details, '$.address.phone')") }} as billing_detail_phone
        ,{{ cents_to_usd('application_fee_amount') }} as application_fee_amount
        ,{{ clean_strings('calculated_statement_descriptor') }} calculated_statement_descriptor
        ,captured as is_captured
        ,TIMESTAMP_SECONDS(created) as created_at_utc
        ,{{ clean_strings('currency') }} as currency
        ,{{ clean_strings('description') }} as charge_description
        ,null as destination
        ,{{ clean_strings('failure_code') }} as failure_code
        ,{{ clean_strings('failure_message') }} as failure_message
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(fraud_details, '$.user_report')") }} as fraud_details_user_report
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(fraud_details, '$.stripe_report')") }} as fraud_details_stripe_report
        ,livemode as is_livemode
        ,JSON_EXTRACT(metadata, '$.variant') as metadata
        ,null as on_behalf_of
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(fraud_details, '$.network_status')") }} as outcome_network_status
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(fraud_details, '$.reason')") }} as outcome_reason
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(fraud_details, '$.risk_level')") }} as outcome_risk_level
        ,JSON_EXTRACT_SCALAR(fraud_details, '$.risk_score') as outcome_risk_score
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(fraud_details, '$.seller_message')") }} as outcome_seller_message
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(fraud_details, '$.type')") }} as outcome_type
        ,paid as is_paid
        ,{{ clean_strings('receipt_email') }} as receipt_email
        ,receipt_number
        ,{{ clean_strings('receipt_url') }} as receipt_url
        ,refunded as is_refunded
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.address.city')" ) }} as shipping_address_city
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.address.country')" ) }} as shipping_address_country
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.address.line_1')" ) }} as shipping_address_line_1
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.address.line_2')" ) }} as shipping_address_line_2
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.address.postal_code')" ) }} as shipping_address_postal_code
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.address.state')" ) }} as shipping_address_state
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.carrier')" ) }} as shipping_carrier
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.name')" ) }} as shipping_name
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.phone')" ) }} as shipping_phone
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(shipping, '$.tracking_number')" ) }} as shipping_tracking_number
        --,card_id
        --,bank_account_id
        ,source.id  as source_id
        --,JSON_EXTRACT(source, '$.transfer') as source_transfer
        ,statement_descriptor
        ,{{ clean_strings('status') }} as charge_status
        --,transfer_data_destination
        --,transfer_group
        ,balance_transaction
        ,customer as customer_id 
        ,invoice as invoice_id
        ,payment_intent as payment_intent_id
        ,payment_method as payment_method_id
        --,transfer_id
        --,rule_rule
        --,rule_id
        --,rule_action
        --,rule_predicate

    from source

)

select * from renamed

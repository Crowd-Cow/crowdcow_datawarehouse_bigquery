with source as (

    select * from {{ source('stripe', 'refund') }}

),

renamed as (

    select
        id as refund_id
        ,{{ cents_to_usd('amount') }} as refund_amount_usd
        ,created as created_at_utc
        ,{{ clean_strings('currency') }} as currency
        ,{{ clean_strings('reason') }} as refund_reason
        ,receipt_number
        ,{{ clean_strings('status') }} as refund_status
        ,balance_transaction_id as stripe_balance_transaction_id
        ,charge_id as stripe_charge_id

    from source

)

select * from renamed


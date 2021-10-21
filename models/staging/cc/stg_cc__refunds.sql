with source as (

    select * from {{ source('cc', 'refunds') }} where not _fivetran_deleted

),

renamed as (

    select
        id as refund_id
        ,{{ clean_strings('reason') }} as refund_reason
        ,{{ clean_strings('stripe_balance_transaction') }} as stripe_balance_transaction_id
        ,{{ clean_strings('stripe_refund_id') }} as stripe_refund_id
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,refund_created_at as refund_created_at_utc
        ,{{ clean_strings('stripe_charge_id') }} as stripe_charge_id
        ,order_id
        ,{{ cents_to_usd('amount_in_cents') }} as refund_amount_usd

    from source
    where not _fivetran_deleted

)

select * from renamed


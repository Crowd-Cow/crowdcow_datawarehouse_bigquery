with source as (

    select * from {{ source('cc', 'cow_cash_entries') }} 

),

renamed as (

    select
        id as cow_cash_id
        ,user_id
        ,credit_id
        ,given_by_user_id
        ,{{ clean_strings('message') }} as cow_cash_message
        ,{{ cents_to_usd('amount_frauded_in_cents') }} as amount_frauded_usd
        ,{{ cents_to_usd('amount_expired_in_cents') }} as amount_expired_usd
        ,{{ cents_to_usd('amount_used_in_cents') }} as amount_used_usd
        ,frauded_at as frauded_at_utc
        ,{{ clean_strings('entry_type') }} as entry_type
        ,gift_card_id
        --,finance_reporting_type
        ,updated_at as updated_at_utc
        ,cow_cash_entry_source_id
        ,order_id
        ,from_order_id
        ,created_at as created_at_utc
        ,{{ cents_to_usd('amount_in_cents') }} as cow_cash_amount_usd
        ,expires_at as expires_at_utc
    from source

)

select * from renamed


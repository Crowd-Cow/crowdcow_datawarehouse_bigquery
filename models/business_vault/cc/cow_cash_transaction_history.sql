with

cow_cash_entry as ( select * from {{ ref('stg_cc__cow_cash_entries') }} )

,cow_cash as (
    select
        cow_cash_id
        ,user_id
        ,credit_id
        ,given_by_user_id
        ,gift_card_id
        ,cow_cash_entry_source_id
        ,order_id
        ,cow_cash_message
        ,entry_type
        ,cow_cash_amount_usd
        ,amount_used_usd
        ,expires_at_utc
        ,created_at_utc
        ,updated_at_utc
    from staging.stg_cc__cow_cash_entries
)

select * from cow_cash

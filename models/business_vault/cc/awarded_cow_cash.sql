with

cow_cash as ( select * from {{ ref('stg_cc__cow_cash_entries') }} )

select
    cow_cash_id
    ,user_id
    ,given_by_user_id
    ,from_order_id
    ,cow_cash_message
    ,entry_type
    ,cow_cash_amount_usd
    ,created_at_utc
    ,updated_at_utc
    ,expires_at_utc
from cow_cash
where cow_cash_entry_source_id is null
    and entry_type not in ('GIFT_CARD','BULK_ORDER')

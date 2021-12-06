with

credit as ( select * from {{ ref('stg_cc__credits') }} )
,cow_cash as ( select * from {{ ref('stg_cc__cow_cash_entries') }} )

,add_cow_cash_information as (
    select
        credit.credit_id
        ,credit.promotion_id
        ,credit.user_id
        ,credit.order_id
        ,credit.cow_cash_entry_source_id
        ,credit.credit_type
        ,awarded_cow_cash.entry_type as awarded_cow_cash_entry_type
        ,credit.credit_description
        ,awarded_cow_cash.cow_cash_message as awarded_cow_cash_message
        ,credit.credit_discount_usd
        ,credit.discount_percent
        ,credit.is_hidden_from_user
        ,credit.is_controlled_by_promotion
        ,credit.created_at_utc
        ,credit.updated_at_utc
    from credit
        left join cow_cash as awarded_cow_cash on credit.cow_cash_entry_source_id = awarded_cow_cash.cow_cash_id
)

select * from add_cow_cash_information
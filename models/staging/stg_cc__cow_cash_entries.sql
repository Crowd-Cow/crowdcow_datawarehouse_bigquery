with source as (

    select * from {{ source('cc', 'cow_cash_entries') }}

),

renamed as (

    select
        cow_cash_id,
        user_id,
        credit_id,
        given_by_user_id,
        cow_cash_message,
        amount_frauded_in_cents,
        amount_expired_in_cents,
        amount_used_in_cents,
        frauded_at,
        entry_type,
        gift_card_id,
        finance_reporting_type,
        updated_at,
        cow_cash_entry_source_id,
        order_id,
        from_order_id,
        created_at,
        amount_in_cents,
        expires_at,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed


with

source as ( select * from {{ source('cc','referrals') }} where not _fivetran_deleted )


,referrals as (
    select
        created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,purchased_at as purchased_at_utc
        ,from_user_id as referrer_user_id
        ,user_id as referee_user_id
        ,to_email as referee_email
        ,{{ cents_to_usd('earned_amount_in_cents') }} as earned_amount_usd

    from source
)

select * from referrals

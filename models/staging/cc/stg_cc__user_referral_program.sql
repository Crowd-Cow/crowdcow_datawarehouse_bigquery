with

source as ( select * from {{ source('cc','user_referral_programs') }} )


,referrals as (
    select
        id as id 
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,user_id 
        ,{{ clean_strings('referral_program') }} as referral_program

    from source
)

select * from referrals

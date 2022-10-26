with

reward as ( select * from {{ source('cc', 'reward_points') }} where not _fivetran_deleted )

,renamed as (
    select
        id as reward_point_id
        ,created_at as created_at_utc
        ,user_id
        ,giver_id
        ,{{ clean_strings('reason') }} as reward_reason
        ,granted_at as granted_at_utc
        ,{{ clean_strings('rewards_program') }} as rewards_program
        ,updated_at as updated_at_utc
        ,order_id
        ,{{ cents_to_usd('amount') }} as reward_spend_amount
    from reward
)

select * from renamed

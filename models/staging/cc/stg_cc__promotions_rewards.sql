with

source as ( select * from {{ source('cc', 'promotions_rewards') }} )

,promotions_rewards as (
    select
        id
        ,updated_at
        ,promotions_promotion_id
        ,created_at
        ,{{ clean_strings('type') }} as type
        ,name
    from source
)

select * from promotions_rewards

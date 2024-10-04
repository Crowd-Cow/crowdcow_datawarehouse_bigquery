with

source as ( select * from {{ source('cc', 'promotions_effects') }}   )

,promotions_effects as (
    select
        id
        ,updated_at
        ,promotions_promotion_id
        ,created_at
        ,{{ clean_strings('type') }} as type
        ,name
    from source
)

select * from promotions_effects

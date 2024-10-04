with

source as ( select * from {{ source('cc', 'promotions_promotions') }}  )

,promotions_promotions as (
    SELECT 
        id,
        updated_at,
        starts_at,
        claimable_window_in_days,
        must_be_claimed,
        ends_at,
        created_at,
        name,
        must_be_applied_by_user,
        token
    FROM source

)

SELECT * from promotions_promotions


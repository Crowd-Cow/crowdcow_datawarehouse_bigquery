with

source as ( select * from {{ source('cc', 'promotions_displays') }} where not _fivetran_deleted  )

,promotions_displays as (
    select
        id as id    
        ,created_at as created_at_utc
        ,user_id 
        ,updated_at as updated_at_utc
        ,promotions_promotion_id
    from source
)

select * from promotions_displays

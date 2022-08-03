with

source as ( select * from {{ source('zendesk', 'user') }} )

,renamed as (
    select
        id as user_id
        ,{{ clean_strings('name') }} as user_name
        ,{{ clean_strings('email') }} as user_email
        ,active as is_active
        ,created_at as created_at_utc
    from source
)

select * from renamed

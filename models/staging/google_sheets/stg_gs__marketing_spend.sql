with

source as ( select * from {{ source('google_sheets', 'marketing_spend') }} )

,renamed as (
    select
        channel
        ,date(week) AS week
        ,spend
    from source
)

select * from renamed

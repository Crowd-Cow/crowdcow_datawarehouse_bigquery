with

source as ( select * from {{ source('google_sheets', 'marketing_spend') }} )

,renamed as (
    select
        channel
        ,to_date(week,'mm/dd/yyyy') as week
        ,spend
    from source
)

select * from renamed

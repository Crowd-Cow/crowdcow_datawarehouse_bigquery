with 
    source as (select * from {{ source('google_sheets', 'daily_cac_manual') }} )

    ,renamed as (
        select
            date(date) as date,
            channel,
            action,
            amount,
            new_customers   
        FROM source 
    )

    select * from renamed 
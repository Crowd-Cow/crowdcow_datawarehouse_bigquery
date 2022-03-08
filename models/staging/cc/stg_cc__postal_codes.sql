with 

source as ( select * from {{ source('cc', 'postal_codes') }} where not _fivetran_deleted )

,renamed as (

    select
        id as postal_code_id
        ,{{ clean_strings('area_name') }} as area_name
        ,population
        ,{{ clean_strings('state_name') }} as state_name
        ,dma_id
        ,latitude
        ,longitude
        ,{{ clean_strings('dma_name') }} as dma_name
        ,postal_code
        ,{{ clean_strings('city_name') }} as city_name
        ,{{ clean_strings('time_zone_name') }} as time_zone_name
        ,updated_at as updated_at_utc
        ,{{ clean_strings('state_code') }} as state_code
        ,{{ cents_to_usd('median_income_cents') }} as median_income
        ,created_at as created_at_utc
        ,{{ cents_to_usd('mean_income_cents') }} as mean_income
        ,{{ clean_strings('po_name') }} as po_name

    from source

)

select * from renamed

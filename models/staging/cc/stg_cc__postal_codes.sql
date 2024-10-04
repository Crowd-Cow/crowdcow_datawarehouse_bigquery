with 

postal_code as ( select * from {{ source('cc', 'postal_codes') }} where not _fivetran_deleted )
,lookup as ( select * from {{ source('cc', 'postal_code_lookup') }} )

,renamed as (

    select
        postal_code.id as postal_code_id
        ,{{ clean_strings('postal_code.area_name') }} as area_name
        ,postal_code.population
        ,{{ clean_strings('postal_code.state_name') }} as state_name
        ,{{ clean_strings('lookup.county_name') }} as county_name
        ,postal_code.dma_id
        ,postal_code.latitude
        ,postal_code.longitude
        ,{{ clean_strings('postal_code.dma_name') }} as dma_name
        ,postal_code.postal_code
        ,{{ clean_strings('postal_code.city_name') }} as city_name
        ,{{ clean_strings('postal_code.time_zone_name') }} as time_zone_name
        ,postal_code.updated_at as updated_at_utc
        ,coalesce(
            {{ clean_strings('lookup.state_code') }}
            ,{{ clean_strings('postal_code.state_code') }}
        ) as state_code
        ,{{ cents_to_usd('postal_code.median_income_cents') }} as median_income
        ,postal_code.created_at as created_at_utc
        ,{{ cents_to_usd('postal_code.mean_income_cents') }} as mean_income
        ,{{ clean_strings('postal_code.po_name') }} as po_name

    from postal_code
        left join lookup on postal_code.postal_code = lookup.postal_code

)

select * from renamed

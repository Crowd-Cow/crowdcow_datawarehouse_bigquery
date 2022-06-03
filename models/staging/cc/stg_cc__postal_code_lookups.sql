with 

source as ( select * from {{ source('cc', 'postal_code_lookup') }} )

,renamed as (
    select
        id as postal_code_lookup_id
        ,postal_code
        ,{{ clean_strings('city_name') }} as city_name
        ,{{ clean_strings('county_name') }} as county_name
        ,{{ clean_strings('state_code') }} as state_code
        ,{{ clean_strings('time_zone_name') }} as time_zone_name
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,latitude
        ,longitude
        ,median_income
        ,population
        ,median_income_clean
        ,population_clean
        ,{{ clean_strings('median_income_bucket') }} as median_income_bucket
        ,{{ clean_strings('designation') }} as designation
        ,dma_id
        ,{{ clean_strings('po_name') }} as po_name
        ,{{ clean_strings('dma_name') }} as dma_name
        ,{{ clean_strings('dma_state') }} as dma_state
    from source
)

select * from renamed

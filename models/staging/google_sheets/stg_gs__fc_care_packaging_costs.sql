with

source as ( select * from {{ source('google_sheets', 'fc_care_packaging_costs') }} )

,renamed as (
    select {{ clean_strings('FULFILLMENT_CENTER_NAME') }} as fc_name
        ,MONTH::date as month_of_costs  
        ,{{ clean_strings('BOX_TYPE') }} as box_type
        ,COST_USD::number as cost_usd
        ,{{ clean_strings('COST_TYPE') }} as cost_type
        ,LABOR_HOURS::number as labor_hours
    from source
)

select * from renamed
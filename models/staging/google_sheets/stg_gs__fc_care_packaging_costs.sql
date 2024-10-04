with

source as ( select * from {{ source('google_sheets', 'fc_care_packaging_costs') }} )

,renamed as (
    SELECT 
        {{ clean_strings('FULFILLMENT_CENTER_NAME') }} AS fc_name,
        CAST(NULLIF(fc_id_auto_populated, 'NULL') AS NUMERIC) AS fc_id,
        CAST(MONTH AS TIMESTAMP) AS month_of_costs,  -- Assuming 'MONTH' is a valid timestamp or needs context clarification
        {{ clean_strings('BOX_TYPE') }} AS box_type,
        CAST(COST_USD AS FLOAT64) AS cost_usd,
        {{ clean_strings('COST_TYPE') }} AS cost_type,
        CAST(LABOR_HOURS AS NUMERIC) AS labor_hours
    FROM 
        source
)

select * from renamed
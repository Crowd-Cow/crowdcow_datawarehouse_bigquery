with 
    source as (select * from {{ source('google_sheets', 'marketing_plan_data') }} )

    ,renamed as (
        select
            EXTRACT(WEEK FROM week_begining) AS fiscal_week_number,
            CAST(week_begining AS DATE) AS week_begining,
            CAST(traffic AS INT64) AS traffic,
            CAST(new_customers AS INT64) AS new_customers,
            CAST(ncsr AS FLOAT64) AS ncsr,
            CAST(conversion_rate AS FLOAT64) AS conversion_rate,
            CAST(sem AS INT64) AS sem,
            CAST(affiliates AS INT64) AS affiliates,
            CAST(offline AS INT64) AS offline,
            CAST(corp_gifting AS INT64) AS corp_gifting,
            CAST(other AS INT64) AS other,
            CAST(total_marketing_spend AS INT64) AS total_marketing_spend,
            CAST(cac AS INT64) AS cac,
            CAST(cac___excl_promo_costs AS INT64) AS cac_excl_promo_costs
        FROM source 
    )

    select * from renamed 
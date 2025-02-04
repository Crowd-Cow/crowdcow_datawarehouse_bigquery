with 
    source as (select * from {{ source('google_sheets', 'marketing_plan_data') }} )

    ,renamed as (
        select
            CAST(fiscal_week as INT64) AS fiscal_week_number
            ,CAST(fiscal_year as INT64) AS fiscal_year
            ,CAST(week_start AS DATE) AS week_begining
            ,CAST(activation_rate as FLOAT64) as activation_rate
            ,CAST(affiliate as INT64) as new_customers_affiliate
            ,CAST(amex as INT64) as new_customers_amex
            ,CAST(direct as INT64) as new_customers_direct
            ,CAST(email  as INT64) as new_customers_email
            ,CAST(organic_social  as INT64) as new_customers_organic_social
            ,CAST(other  as INT64) as new_customers_other
            ,CAST(sem  as INT64) as new_customers_sem
            ,CAST(seo  as INT64) as new_customers_seo
            ,CAST(social  as INT64) as new_customers_social
            ,CAST(total  as INT64) as new_customers_total

            ,CAST(corp_gifting_orders as INT64) as corp_gifting_orders
            ,CAST(existing_alc_orders as INT64) as existing_alc_orders
            ,CAST(existing_member_orders as INT64) as existing_member_orders
            ,CAST(new_alc_orders as INT64) as new_alc_orders
            ,CAST(new_member_orders as INT64) as new_member_orders


            ,CAST(pdp_prospect_traffic_conversion_rate as FLOAT64) as pdp_prospect_traffic_conversion_rate
            ,CAST(sem_prospect_traffic_conversion_rate as FLOAT64) as sem_prospect_traffic_conversion_rate


        FROM source 
    )

    select * from renamed 
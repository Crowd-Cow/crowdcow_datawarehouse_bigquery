with 
    source as (select * from {{ source('google_sheets', 'marketing_plan_data') }} )

    ,renamed as (
        select
            extract(week from week_begining) as fiscal_week_number,
            week_begining::date as week_begining,
            traffic::number as traffic,
            new_customers::number as new_customers,
            ncsr::float as ncsr ,
            conversion_rate::float as conversion_rate,
            sem::number as sem,
            affiliates::number as affiliates,
            offline::number as offline,
            corp_gifting::number as corp_gifting,
            other::number as other,
            total_marketing_spend::number as total_marketing_spend,
            cac::number as cac,
            cac_excl_promo_costs::number as cac_excl_promo_costs
        from source 
    )

    select * from renamed 
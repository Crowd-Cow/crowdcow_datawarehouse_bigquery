{{ config(
  enabled=false
) }}
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
            ,CAST(orders___total_forecast as INT64) as total_orders



            ,CAST(pdp_prospect_traffic_conversion_rate as FLOAT64) as pdp_prospect_traffic_conversion_rate
            ,CAST(sem_prospect_traffic_conversion_rate as FLOAT64) as sem_prospect_traffic_conversion_rate

            ,CAST(_90_day_sub_start as INT64) as _90_day_sub_start
            ,CAST(_90_day_sub_new_first_subscriber as INT64) as _90_day_sub_new_first_subscriber
            ,CAST(_90_day_sub_new_subscription as INT64) as _90_day_sub_new_subscription
            ,CAST(_90_day_sub_reactivated as INT64) as _90_day_sub_reactivated
            ,CAST(_90_day_sub_churn as INT64) as _90_day_sub_churn
            ,CAST(_90_day_sub_week_end as INT64) as _90_day_sub_week_end 
            ,CAST(_90_day_sub_forecasted_churn_rate as INT64) as _90_day_sub_forecasted_churn_rate
            ,CAST(_90_day_sub_forecasted_reactivation_rate as INT64) as _90_day_sub_forecasted_reactivation_rate

            
            ,CAST(_180_day_alc_week_start as INT64) as _180_day_alc_week_start
            ,CAST(_180_day_alc_new as INT64) as _180_day_alc_new
            ,CAST(_180_day_alc_reactivation as INT64) as _180_day_alc_reactivation
            ,CAST(_180_day_alc_churned_180_days as INT64) as _180_day_alc_churned_180_days
            ,CAST(_180_day_alc_week_end as INT64) as _180_day_alc_week_end
            ,CAST(_180_day_alc_forecasted_churn_rate as INT64) as _180_day_alc_forecasted_churn_rate
            ,CAST(_180_day_alc_forecasted_reactivation_rate as INT64) as _180_day_alc_forecasted_reactivation_rate

            ,CAST(sms_audience_size_net_of_unsubscribes as INT64) as sms_audience_size

            ,CAST(sales as INT64) as sales_forecast

            ,CAST(new_attentive_leads_emails as INT64) as new_attentive_leads_emails
            ,CAST(new_customers_from_attentive_leads as INT64) as new_customers_from_attentive_leads

            ,CAST(tocc_sales_to_new_customers_units as INT64) as tocc_sales_to_new_customers_units




        FROM source 
    )

    select * from renamed 
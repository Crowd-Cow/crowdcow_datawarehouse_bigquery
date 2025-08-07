with marketing_plan_data as (select * from {{ ref('stg_gs__marketing_plan_data') }})

,orders as (select * from {{ ref('orders') }} where order_type = 'E-COMMERCE' and IS_PAID_ORDER and not IS_CANCELLED_ORDER)

,fiscal_calendar as (select * from {{ ref('retail_calendar') }} where fiscal_year >= 2025) 

,weekly_calendar AS (
    SELECT
        fiscal_year,
        fiscal_week_num,
        fiscal_quarter,
        fiscal_month,
        day_of_week,
        timestamp(calendar_date) as calendar_date,
        timestamp(date(fc.calendar_date_week_sun)) AS week_start_timestamp,
        timestamp(DATE_ADD(fc.calendar_date_week_sun, INTERVAL 7 DAY)) AS week_end_timestamp,
    FROM fiscal_calendar fc
    
)

   ,dow_orders as (
    SELECT
        retail_calendar.DAY_OF_WEEK AS retail_calendar_day_of_week,
        COUNT(DISTINCT orders.ORDER_ID ) AS total_paid_orders,
        COUNT(DISTINCT case when  orders.is_membership_order then orders.order_id end ) AS total_paid_membership_orders,
        COUNT(DISTINCT case when not orders.is_membership_order then orders.order_id end ) AS total_paid_alc_orders,

        COALESCE(SUM(orders.net_revenue), 0) AS total_paid_net_revenue,
        COALESCE(SUM(CASE when orders.IS_ALA_CARTE_ORDER then orders.net_revenue  ELSE NULL END), 0) AS total_paid_ala_carte_net_revenue,
        COALESCE(SUM(case when orders.IS_MEMBERSHIP_ORDER  then orders.net_revenue end ), 0) AS total_paid_membership_net_revenue,

        COALESCE(SUM(CASE when orders.PAID_ALA_CARTE_ORDER_RANK > 1 and orders.IS_ALA_CARTE_ORDER then orders.net_revenue  ELSE NULL END), 0) AS existing_paid_ala_carte_net_revenue,
        COALESCE(SUM(case when orders.paid_unique_membership_order_rank > 1  and orders.IS_MEMBERSHIP_ORDER  then orders.net_revenue end ), 0) AS existing_paid_membership_net_revenue,
        COALESCE(SUM(CASE when orders.PAID_ALA_CARTE_ORDER_RANK = 1 and orders.IS_ALA_CARTE_ORDER then orders.net_revenue  ELSE NULL END), 0) AS new_paid_ala_carte_net_revenue,  
        COALESCE(SUM(case when orders.paid_membership_order_rank = 1 and orders.IS_MEMBERSHIP_ORDER  then orders.net_revenue end ), 0) AS new_paid_membership_net_revenue

    FROM qa_business_vault.orders
             LEFT JOIN qa_business_vault.retail_calendar AS retail_calendar ON (DATE(timestamp(retail_calendar.CALENDAR_DATE))) = (DATE(orders.ORDER_PAID_AT_UTC , 'America/Los_Angeles'))
    WHERE ((( orders.ORDER_PAID_AT_UTC  ) >= ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), WEEK(SUNDAY), 'America/Los_Angeles'), 'America/Los_Angeles'), INTERVAL -12 WEEK), 'America/Los_Angeles'))) AND ( orders.ORDER_PAID_AT_UTC  ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), WEEK(SUNDAY), 'America/Los_Angeles'), 'America/Los_Angeles'), INTERVAL -12 WEEK), 'America/Los_Angeles'), 'America/Los_Angeles'), INTERVAL 12 WEEK), 'America/Los_Angeles')))))
    --((( orders.ORDER_PAID_AT_UTC  ) >= ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), WEEK(SUNDAY), 'America/Los_Angeles'), 'America/Los_Angeles'), INTERVAL -4 WEEK), 'America/Los_Angeles'))) AND ( orders.ORDER_PAID_AT_UTC  ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), WEEK(SUNDAY), 'America/Los_Angeles'), 'America/Los_Angeles'), INTERVAL -4 WEEK), 'America/Los_Angeles'), 'America/Los_Angeles'), INTERVAL 1 WEEK), 'America/Los_Angeles')))))
    group by 1
)

   ,dow_weight as (
    select
      retail_calendar_day_of_week
     ,total_paid_orders / sum(total_paid_orders) over () as dow_total_order_weight
     ,total_paid_membership_orders / sum(total_paid_membership_orders) over () as  dow_membership_order_weight
     ,total_paid_alc_orders / sum(total_paid_alc_orders) over () as  dow_alc_order_weight
     ,total_paid_ala_carte_net_revenue / sum(total_paid_ala_carte_net_revenue) over () as dow_alc_revenue_weight
     ,total_paid_membership_net_revenue / sum(total_paid_membership_net_revenue) over () as dow_membership_revenue_weight
    from dow_orders
)
,total_weight as (
  select
  sum(total_paid_membership_net_revenue) / sum(total_paid_net_revenue) as total_membership_revenue_weight,
  1 - sum(total_paid_membership_net_revenue) / sum(total_paid_net_revenue) as total_alc_revenue_weight,
  1 - sum(existing_paid_membership_net_revenue) / sum(existing_paid_ala_carte_net_revenue + existing_paid_membership_net_revenue) as total_existing_alc_revenue_weight,
  sum(existing_paid_membership_net_revenue) / sum(existing_paid_ala_carte_net_revenue + existing_paid_membership_net_revenue) as  total_existing_membership_revenue_weight,
  sum(new_paid_ala_carte_net_revenue) / sum(new_paid_ala_carte_net_revenue + new_paid_membership_net_revenue ) as total_new_alc_revenue_weight,
  sum(new_paid_membership_net_revenue) / sum(new_paid_ala_carte_net_revenue + new_paid_membership_net_revenue) as  total_new_membership_revenue_weight
  from dow_orders
)

select
fiscal_week_number
,marketing_plan_data.fiscal_year
,week_begining
,fiscal_quarter
,fiscal_month
,calendar_date
,day_of_week
,activation_rate / 7 as activation_rate
,new_customers_affiliate / 7 as new_customers_affiliate
,new_customers_amex / 7 as new_customers_amex
,new_customers_direct / 7 as new_customers_direct
,new_customers_email / 7 as new_customers_email
,new_customers_organic_social / 7 as new_customers_organic_social
,new_customers_other / 7 as new_customers_other
,new_customers_sem / 7 as new_customers_sem
,new_customers_seo / 7 as new_customers_seo
,new_customers_social / 7 as new_customers_social
,new_customers_total / 7 as new_customers_total

,corp_gifting_orders / 7 as corp_gifting_orders
,existing_alc_orders * dow_alc_order_weight as existing_alc_orders
,existing_member_orders * dow_membership_revenue_weight as existing_member_orders
,new_alc_orders / 7 as new_alc_orders
,new_member_orders / 7 as new_member_orders
,(existing_alc_orders * dow_alc_order_weight) + (existing_member_orders * dow_membership_revenue_weight) + (new_alc_orders / 7 ) + (new_member_orders / 7) as total_orders


,((new_customer_revenue * total_new_membership_revenue_weight)/ 7) + ((existing_customer_revenue * total_existing_membership_revenue_weight) * dow_membership_revenue_weight) + ((new_customer_revenue * total_new_alc_revenue_weight) / 7 ) + ((existing_customer_revenue * total_existing_alc_revenue_weight) * dow_alc_order_weight)    as sales_forecast
,((new_customer_revenue * total_new_membership_revenue_weight)/ 7) + ((existing_customer_revenue * total_existing_membership_revenue_weight) * dow_membership_revenue_weight) as total_memberships_sales_forecast
,((new_customer_revenue * total_new_alc_revenue_weight) / 7 ) + ((existing_customer_revenue * total_existing_alc_revenue_weight) * dow_alc_order_weight)  as total_alc_sales_forecast
,(existing_customer_revenue * total_existing_membership_revenue_weight) * dow_membership_revenue_weight as existing_memberships_sales_forecast
,(existing_customer_revenue * total_existing_alc_revenue_weight) * dow_alc_order_weight as existing_alc_sales_forecast
,(new_customer_revenue * total_new_membership_revenue_weight)  / 7  as new_memberships_sales_forecast
,(new_customer_revenue * total_new_alc_revenue_weight) / 7  as new_alc_sales_forecast

,(new_customer_revenue * total_new_alc_revenue_weight) / (new_customer_orders * total_new_alc_revenue_weight)  as new_alc_aov_forecast
,(existing_customer_revenue * total_existing_alc_revenue_weight) / (existing_customer_orders * total_existing_alc_revenue_weight)  as existing_alc_aov_forecast
,(new_customer_revenue * total_new_membership_revenue_weight) / (new_customer_orders * total_new_membership_revenue_weight)  as new_memberships_aov_forecast
,(existing_customer_revenue * total_existing_membership_revenue_weight) / (existing_customer_orders * total_existing_membership_revenue_weight)  as existing_memberships_aov_forecast






,pdp_prospect_traffic_conversion_rate / 7 as pdp_prospect_traffic_conversion_rate
,sem_prospect_traffic_conversion_rate / 7 as sem_prospect_traffic_conversion_rate

,_90_day_sub_start / 7 as _90_day_sub_start
,_90_day_sub_new_first_subscriber / 7 as _90_day_sub_new_first_subscriber
,_90_day_sub_new_subscription / 7 as _90_day_sub_new_subscription
,_90_day_sub_reactivated / 7 as _90_day_sub_reactivated
,_90_day_sub_churn / 7 as _90_day_sub_churn
,_90_day_sub_week_end / 7 as  _90_day_sub_week_end
,_90_day_sub_forecasted_churn_rate / 7 as _90_day_sub_forecasted_churn_rate
,_90_day_sub_forecasted_reactivation_rate / 7 as _90_day_sub_forecasted_reactivation_rate


,_90_day_alc_week_start / 7 as _90_day_alc_week_start
,_90_day_alc_new / 7 as _90_day_alc_new
,_90_day_alc_reactivation / 7 as _90_day_alc_reactivation
,_90_day_alc_churned_90_days / 7 as _90_day_alc_churned_90_days
,_90_day_alc_week_end / 7 as _90_day_alc_week_end
,_90_day_alc_forecasted_churn_rate / 7 as _90_day_alc_forecasted_churn_rate
,_90_day_alc_forecasted_reactivation_rate / 7 as _90_day_alc_forecasted_reactivation_rate

,sms_audience_size / 7 as sms_audience_size

,new_attentive_leads_emails / 7 as new_attentive_leads_emails
,new_customers_from_attentive_leads / 7 as new_customers_from_attentive_leads

,tocc_sales_to_new_customers_units / 7 as tocc_sales_to_new_customers_units

,aov / 7 as aov
,active_customer_count / 7 as active_customer_count
,paid_marketing_spend / 7 as paid_marketing_spend
,Marginal_cac / 7 as Marginal_cac

from marketing_plan_data
left join weekly_calendar on weekly_calendar.fiscal_year = marketing_plan_data.fiscal_year and weekly_calendar.fiscal_week_num = marketing_plan_data.fiscal_week_number
left join dow_weight on dow_weight.retail_calendar_day_of_week = weekly_calendar.day_of_week
cross join total_weight
order by calendar_date desc
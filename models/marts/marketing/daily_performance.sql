with 
orders as (select * from {{ ref('orders') }} where order_type = 'E-COMMERCE' and IS_PAID_ORDER and not IS_CANCELLED_ORDER)
,plan_data as (select * from {{ ref('marketing_plan_data')}})
,fiscal_calendar as (select * from {{ ref('retail_calendar') }}) 


-- Current week performance
,performance as (
SELECT
    timestamp(fiscal_calendar.calendar_date) as calendar_date,
    fiscal_calendar.fiscal_week_num,
    fiscal_calendar.fiscal_month,
    fiscal_calendar.fiscal_quarter,
    fiscal_calendar.DAY_OF_WEEK,
    --orders
    COUNT(DISTINCT orders.ORDER_ID ) AS total_paid_orders,
    COUNT(DISTINCT case when  orders.is_membership_order then orders.order_id end ) AS total_paid_membership_orders,
    COUNT(DISTINCT case when not orders.is_membership_order then orders.order_id end ) AS total_paid_alc_orders,
    COUNT(DISTINCT case when orders.PAID_ALA_CARTE_ORDER_RANK > 1 and orders.IS_ALA_CARTE_ORDER then orders.order_id  else null end) AS existing_paid_ala_carte_orders,
    COUNT(DISTINCT case when orders.paid_unique_membership_order_rank > 1  and orders.IS_MEMBERSHIP_ORDER  then orders.order_id end ) AS existing_paid_membership_orders,
    COUNT(DISTINCT case when orders.PAID_ALA_CARTE_ORDER_RANK = 1 and orders.IS_ALA_CARTE_ORDER then orders.order_id  else null end) AS new_paid_ala_carte_orders,
    COUNT(DISTINCT case when orders.paid_membership_order_rank = 1 and paid_order_rank = 1 and orders.IS_MEMBERSHIP_ORDER  then orders.order_id end ) AS new_paid_membership_orders,
    --revenue
    COALESCE(SUM(orders.net_revenue), 0) AS total_paid_net_revenue,
    COALESCE(SUM(case when orders.IS_ALA_CARTE_ORDER then orders.net_revenue  else null end), 0) AS total_paid_ala_carte_net_revenue,
    COALESCE(SUM(case when orders.IS_MEMBERSHIP_ORDER  then orders.net_revenue end ), 0) AS total_paid_membership_net_revenue,
    COALESCE(SUM(case when orders.PAID_ALA_CARTE_ORDER_RANK > 1 and orders.IS_ALA_CARTE_ORDER then orders.net_revenue  else null end), 0) AS existing_paid_ala_carte_net_revenue,
    COALESCE(SUM(case when orders.paid_unique_membership_order_rank > 1  and orders.IS_MEMBERSHIP_ORDER  then orders.net_revenue end ), 0) AS existing_paid_membership_net_revenue,
    COALESCE(SUM(case when orders.PAID_ALA_CARTE_ORDER_RANK = 1 and orders.IS_ALA_CARTE_ORDER then orders.net_revenue  else null end), 0) AS new_paid_ala_carte_net_revenue,
    COALESCE(SUM(case when orders.paid_membership_order_rank = 1 and orders.IS_MEMBERSHIP_ORDER  then orders.net_revenue end ), 0) AS new_paid_membership_net_revenue

FROM orders
LEFT JOIN fiscal_calendar ON (DATE(timestamp(fiscal_calendar.CALENDAR_DATE))) = (DATE(orders.ORDER_PAID_AT_UTC , 'America/Los_Angeles'))
group by 1,2,3,4,5
order by 1 desc
)

,plan_performance as (
    select 
        performance.calendar_date,
        performance.fiscal_week_num,
        performance.fiscal_month,
        performance.fiscal_quarter,
        performance.DAY_OF_WEEK,
        COALESCE(SUM(performance.total_paid_orders),0) as total_paid_orders,
        COALESCE(SUM(performance.total_paid_membership_orders),0) as total_paid_membership_orders,
        COALESCE(SUM(performance.total_paid_alc_orders),0) as total_paid_alc_orders,
        COALESCE(SUM(performance.existing_paid_ala_carte_orders),0) as existing_paid_ala_carte_orders,
        COALESCE(SUM(performance.existing_paid_membership_orders),0) as existing_paid_membership_orders,
        COALESCE(SUM(performance.new_paid_ala_carte_orders),0) as new_paid_ala_carte_orders,
        COALESCE(SUM(performance.new_paid_membership_orders),0) as new_paid_membership_orders,
        COALESCE(SUM(performance.total_paid_net_revenue),0) as total_paid_net_revenue,
        COALESCE(SUM(performance.total_paid_ala_carte_net_revenue),0) as total_paid_ala_carte_net_revenue,
        COALESCE(SUM(performance.total_paid_membership_net_revenue),0) as total_paid_membership_net_revenue,
        COALESCE(SUM(performance.existing_paid_ala_carte_net_revenue),0) as existing_paid_ala_carte_net_revenue,
        COALESCE(SUM(performance.existing_paid_membership_net_revenue),0) as existing_paid_membership_net_revenue,
        COALESCE(SUM(performance.new_paid_ala_carte_net_revenue),0) as new_paid_ala_carte_net_revenue,
        COALESCE(SUM(performance.new_paid_membership_net_revenue),0) as new_paid_membership_net_revenue,
        --plan orders
        COALESCE(SUM(plan_data.total_orders),0) as plan_total_orders,
        COALESCE(SUM(plan_data.existing_member_orders),0) + COALESCE(SUM(new_member_orders),0) as plan_total_member_orders,
        COALESCE(SUM(plan_data.existing_alc_orders),0) + COALESCE(SUM(new_alc_orders),0) as plan_total_alc_orders,
        COALESCE(SUM(plan_data.existing_member_orders),0) as plan_existing_member_orders,
        COALESCE(SUM(plan_data.existing_alc_orders),0) as plan_existing_alc_orders,
        COALESCE(SUM(plan_data.new_member_orders),0) as plan_new_member_orders,
        COALESCE(SUM(plan_data.new_alc_orders),0) as plan_new_alc_orders,
        --plan revenue
        COALESCE(SUM(plan_data.sales_forecast),0) as plan_total_sales_forecast,
        COALESCE(SUM(plan_data.total_memberships_sales_forecast),0) as plan_total_memberships_sales_forecast,
        COALESCE(SUM(plan_data.total_alc_sales_forecast),0) as plan_total_alc_sales_forecast,
        COALESCE(SUM(plan_data.existing_memberships_sales_forecast),0) as plan_existing_memberships_sales_forecast,
        COALESCE(SUM(plan_data.existing_alc_sales_forecast),0) as plan_existing_alc_sales_forecast,
        COALESCE(SUM(plan_data.new_memberships_sales_forecast),0) as plan_new_memberships_sales_forecast,
        COALESCE(SUM(plan_data.new_alc_sales_forecast),0) as plan_new_alc_sales_forecast,
    from performance 
    LEFT JOIN plan_data ON  (DATE(timestamp(performance.CALENDAR_DATE))) = (DATE(plan_data.calendar_date))
    group by 1,2,3,4,5

)


,performance_previous_week as (
    select 
        DAY_OF_WEEK as pw_day_of_week
        ,total_paid_orders as pw_total_paid_orders
        ,total_paid_membership_orders as pw_total_paid_membership_orders
        ,total_paid_alc_orders as pw_total_paid_alc_orders
        ,existing_paid_ala_carte_orders as pw_existing_paid_ala_carte_orders
        ,existing_paid_membership_orders as pw_existing_paid_membership_orders
        ,new_paid_ala_carte_orders as pw_new_paid_ala_carte_orders
        ,new_paid_membership_orders as pw_new_paid_membership_orders
        ,total_paid_net_revenue as pw_total_paid_net_revenue
        ,total_paid_ala_carte_net_revenue as pw_total_paid_ala_carte_net_revenue
        ,total_paid_membership_net_revenue as pw_total_paid_membership_net_revenue
        ,existing_paid_ala_carte_net_revenue as pw_existing_paid_ala_carte_net_revenue
        ,existing_paid_membership_net_revenue as pw_existing_paid_membership_net_revenue
        ,new_paid_ala_carte_net_revenue as pw_new_paid_ala_carte_net_revenue
        ,new_paid_membership_net_revenue as pw_new_paid_membership_net_revenue
        ,plan_total_orders as pw_plan_total_orders
        ,plan_total_member_orders as pw_plan_total_member_orders
        ,plan_total_alc_orders as pw_plan_total_alc_orders
        ,plan_existing_member_orders as pw_plan_existing_member_orders
        ,plan_existing_alc_orders as pw_plan_existing_alc_orders
        ,plan_new_member_orders as pw_plan_new_member_orders
        ,plan_new_alc_orders as pw_plan_new_alc_orders
        ,plan_total_sales_forecast as pw_plan_total_sales_forecast
        ,plan_total_memberships_sales_forecast as pw_plan_total_memberships_sales_forecast
        ,plan_total_alc_sales_forecast as pw_plan_total_alc_sales_forecast
        ,plan_existing_memberships_sales_forecast as pw_plan_existing_memberships_sales_forecast
        ,plan_existing_alc_sales_forecast as pw_plan_existing_alc_sales_forecast
        ,plan_new_memberships_sales_forecast as pw_plan_new_memberships_sales_forecast
        ,plan_new_alc_sales_forecast as pw_plan_new_alc_sales_forecast
        ,sum(total_paid_orders) over ( order by day_of_week asc ) as pw_cumulative_total_paid_orders
        ,sum(total_paid_membership_orders) over ( order by day_of_week asc ) as pw_cumulative_total_paid_membership_orders
        ,sum(total_paid_alc_orders) over ( order by day_of_week asc ) as pw_cumulative_total_paid_alc_orders
        ,sum(existing_paid_ala_carte_orders) over ( order by day_of_week asc ) as pw_cumulative_existing_paid_ala_carte_orders
        ,sum(existing_paid_membership_orders) over ( order by day_of_week asc ) as pw_cumulative_existing_paid_membership_orders
        ,sum(new_paid_ala_carte_orders) over ( order by day_of_week asc ) as pw_cumulative_new_paid_ala_carte_orders
        ,sum(new_paid_membership_orders) over ( order by day_of_week asc ) as pw_cumulative_new_paid_membership_orders
        ,sum(total_paid_net_revenue) over ( order by day_of_week asc ) as pw_cumulative_total_paid_net_revenue
        ,sum(total_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as pw_cumulative_total_paid_ala_carte_net_revenue
        ,sum(total_paid_membership_net_revenue) over ( order by day_of_week asc ) as pw_cumulative_total_paid_membership_net_revenue
        ,sum(existing_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as pw_cumulative_existing_paid_ala_carte_net_revenue
        ,sum(existing_paid_membership_net_revenue) over ( order by day_of_week asc ) as pw_cumulative_existing_paid_membership_net_revenue
        ,sum(new_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as pw_cumulative_new_paid_ala_carte_net_revenue
        ,sum(new_paid_membership_net_revenue) over ( order by day_of_week asc ) as pw_cumulative_new_paid_membership_net_revenue
        ,sum(plan_total_orders) over ( order by day_of_week asc ) as pw_cumulative_plan_total_orders
        ,sum(plan_total_member_orders) over ( order by day_of_week asc ) as pw_cumulative_plan_total_member_orders
        ,sum(plan_total_alc_orders) over ( order by day_of_week asc ) as pw_cumulative_plan_total_alc_orders
        ,sum(plan_existing_member_orders) over ( order by day_of_week asc ) as pw_cumulative_plan_existing_member_orders
        ,sum(plan_existing_alc_orders) over ( order by day_of_week asc ) as pw_cumulative_plan_existing_alc_orders
        ,sum(plan_new_member_orders) over ( order by day_of_week asc ) as pw_cumulative_plan_new_member_orders
        ,sum(plan_new_alc_orders) over ( order by day_of_week asc ) as pw_cumulative_plan_new_alc_orders
        ,sum(plan_total_sales_forecast) over ( order by day_of_week asc ) as pw_cumulative_plan_total_sales_forecast
        ,sum(plan_total_memberships_sales_forecast) over ( order by day_of_week asc ) as pw_cumulative_plan_total_memberships_sales_forecast
        ,sum(plan_total_alc_sales_forecast) over ( order by day_of_week asc ) as pw_cumulative_plan_total_alc_sales_forecast
        ,sum(plan_existing_memberships_sales_forecast) over ( order by day_of_week asc ) as pw_cumulative_plan_existing_memberships_sales_forecast
        ,sum(plan_existing_alc_sales_forecast) over ( order by day_of_week asc ) as pw_cumulative_plan_existing_alc_sales_forecast
        ,sum(plan_new_memberships_sales_forecast) over ( order by day_of_week asc ) as pw_cumulative_plan_new_memberships_sales_forecast
        ,sum(plan_new_alc_sales_forecast) over ( order by day_of_week asc ) as pw_cumulative_plan_new_alc_sales_forecast

    from plan_performance
    where ((( calendar_date  ) >= ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY))), INTERVAL -1 WEEK)))) AND ( calendar_date  ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY))), INTERVAL -1 WEEK))), INTERVAL 1 WEEK))))))
)
,performance_last_year as (
    select 
        DAY_OF_WEEK as ly_day_of_week
        ,total_paid_orders as ly_total_paid_orders
        ,total_paid_membership_orders as ly_total_paid_membership_orders
        ,total_paid_alc_orders as ly_total_paid_alc_orders
        ,existing_paid_ala_carte_orders as ly_existing_paid_ala_carte_orders
        ,existing_paid_membership_orders as ly_existing_paid_membership_orders
        ,new_paid_ala_carte_orders as ly_new_paid_ala_carte_orders
        ,new_paid_membership_orders as ly_new_paid_membership_orders
        ,total_paid_net_revenue as ly_total_paid_net_revenue
        ,total_paid_ala_carte_net_revenue as ly_total_paid_ala_carte_net_revenue
        ,total_paid_membership_net_revenue as ly_total_paid_membership_net_revenue
        ,existing_paid_ala_carte_net_revenue as ly_existing_paid_ala_carte_net_revenue
        ,existing_paid_membership_net_revenue as ly_existing_paid_membership_net_revenue
        ,new_paid_ala_carte_net_revenue as ly_new_paid_ala_carte_net_revenue
        ,new_paid_membership_net_revenue as ly_new_paid_membership_net_revenue
        ,plan_total_orders as ly_plan_total_orders
        ,plan_total_member_orders as ly_plan_total_member_orders
        ,plan_total_alc_orders as ly_plan_total_alc_orders
        ,plan_existing_member_orders as ly_plan_existing_member_orders
        ,plan_existing_alc_orders as ly_plan_existing_alc_orders
        ,plan_new_member_orders as ly_plan_new_member_orders
        ,plan_new_alc_orders as ly_plan_new_alc_orders
        ,plan_total_sales_forecast as ly_plan_total_sales_forecast
        ,plan_total_memberships_sales_forecast as ly_plan_total_memberships_sales_forecast
        ,plan_total_alc_sales_forecast as ly_plan_total_alc_sales_forecast
        ,plan_existing_memberships_sales_forecast as ly_plan_existing_memberships_sales_forecast
        ,plan_existing_alc_sales_forecast as ly_plan_existing_alc_sales_forecast
        ,plan_new_memberships_sales_forecast as ly_plan_new_memberships_sales_forecast
        ,plan_new_alc_sales_forecast as ly_plan_new_alc_sales_forecast
        ,sum(total_paid_orders) over ( order by day_of_week asc ) as ly_cumulative_total_paid_orders
        ,sum(total_paid_membership_orders) over ( order by day_of_week asc ) as ly_cumulative_total_paid_membership_orders
        ,sum(total_paid_alc_orders) over ( order by day_of_week asc ) as ly_cumulative_total_paid_alc_orders
        ,sum(existing_paid_ala_carte_orders) over ( order by day_of_week asc ) as ly_cumulative_existing_paid_ala_carte_orders
        ,sum(existing_paid_membership_orders) over ( order by day_of_week asc ) as ly_cumulative_existing_paid_membership_orders
        ,sum(new_paid_ala_carte_orders) over ( order by day_of_week asc ) as ly_cumulative_new_paid_ala_carte_orders
        ,sum(new_paid_membership_orders) over ( order by day_of_week asc ) as ly_cumulative_new_paid_membership_orders
        ,sum(total_paid_net_revenue) over ( order by day_of_week asc ) as ly_cumulative_total_paid_net_revenue
        ,sum(total_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as ly_cumulative_total_paid_ala_carte_net_revenue
        ,sum(total_paid_membership_net_revenue) over ( order by day_of_week asc ) as ly_cumulative_total_paid_membership_net_revenue
        ,sum(existing_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as ly_cumulative_existing_paid_ala_carte_net_revenue
        ,sum(existing_paid_membership_net_revenue) over ( order by day_of_week asc ) as ly_cumulative_existing_paid_membership_net_revenue
        ,sum(new_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as ly_cumulative_new_paid_ala_carte_net_revenue
        ,sum(new_paid_membership_net_revenue) over ( order by day_of_week asc ) as ly_cumulative_new_paid_membership_net_revenue
        ,sum(plan_total_orders) over ( order by day_of_week asc ) as ly_cumulative_plan_total_orders
        ,sum(plan_total_member_orders) over ( order by day_of_week asc ) as ly_cumulative_plan_total_member_orders
        ,sum(plan_total_alc_orders) over ( order by day_of_week asc ) as ly_cumulative_plan_total_alc_orders
        ,sum(plan_existing_member_orders) over ( order by day_of_week asc ) as ly_cumulative_plan_existing_member_orders
        ,sum(plan_existing_alc_orders) over ( order by day_of_week asc ) as ly_cumulative_plan_existing_alc_orders
        ,sum(plan_new_member_orders) over ( order by day_of_week asc ) as ly_cumulative_plan_new_member_orders
        ,sum(plan_new_alc_orders) over ( order by day_of_week asc ) as ly_cumulative_plan_new_alc_orders
        ,sum(plan_total_sales_forecast) over ( order by day_of_week asc ) as ly_cumulative_plan_total_sales_forecast
        ,sum(plan_total_memberships_sales_forecast) over ( order by day_of_week asc ) as ly_cumulative_plan_total_memberships_sales_forecast
        ,sum(plan_total_alc_sales_forecast) over ( order by day_of_week asc ) as ly_cumulative_plan_total_alc_sales_forecast
        ,sum(plan_existing_memberships_sales_forecast) over ( order by day_of_week asc ) as ly_cumulative_plan_existing_memberships_sales_forecast
        ,sum(plan_existing_alc_sales_forecast) over ( order by day_of_week asc ) as ly_cumulative_plan_existing_alc_sales_forecast
        ,sum(plan_new_memberships_sales_forecast) over ( order by day_of_week asc ) as ly_cumulative_plan_new_memberships_sales_forecast
        ,sum(plan_new_alc_sales_forecast) over ( order by day_of_week asc ) as ly_cumulative_plan_new_alc_sales_forecast
    from plan_performance
    where ((( calendar_date  ) >= ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY))), INTERVAL -52 WEEK)))) AND ( calendar_date  ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY))), INTERVAL -52 WEEK))), INTERVAL 1 WEEK))))))
)

,performance_current_week as (
    select 
        calendar_date
        ,fiscal_week_num
        ,fiscal_month
        ,fiscal_quarter
        ,DAY_OF_WEEK as cw_day_of_week
        ,total_paid_orders as cw_total_paid_orders
        ,total_paid_membership_orders as cw_total_paid_membership_orders
        ,total_paid_alc_orders as cw_total_paid_alc_orders
        ,existing_paid_ala_carte_orders as cw_existing_paid_ala_carte_orders
        ,existing_paid_membership_orders as cw_existing_paid_membership_orders
        ,new_paid_ala_carte_orders as cw_new_paid_ala_carte_orders
        ,new_paid_membership_orders as cw_new_paid_membership_orders
        ,total_paid_net_revenue as cw_total_paid_net_revenue
        ,total_paid_ala_carte_net_revenue as cw_total_paid_ala_carte_net_revenue
        ,total_paid_membership_net_revenue as cw_total_paid_membership_net_revenue
        ,existing_paid_ala_carte_net_revenue as cw_existing_paid_ala_carte_net_revenue
        ,existing_paid_membership_net_revenue as cw_existing_paid_membership_net_revenue
        ,new_paid_ala_carte_net_revenue as cw_new_paid_ala_carte_net_revenue
        ,new_paid_membership_net_revenue as cw_new_paid_membership_net_revenue
        ,plan_total_orders as cw_plan_total_orders
        ,plan_total_member_orders as cw_plan_total_member_orders
        ,plan_total_alc_orders as cw_plan_total_alc_orders
        ,plan_existing_member_orders as cw_plan_existing_member_orders
        ,plan_existing_alc_orders as cw_plan_existing_alc_orders
        ,plan_new_member_orders as cw_plan_new_member_orders
        ,plan_new_alc_orders as cw_plan_new_alc_orders
        ,plan_total_sales_forecast as cw_plan_total_sales_forecast
        ,plan_total_memberships_sales_forecast as cw_plan_total_memberships_sales_forecast
        ,plan_total_alc_sales_forecast as cw_plan_total_alc_sales_forecast
        ,plan_existing_memberships_sales_forecast as cw_plan_existing_memberships_sales_forecast
        ,plan_existing_alc_sales_forecast as cw_plan_existing_alc_sales_forecast
        ,plan_new_memberships_sales_forecast as cw_plan_new_memberships_sales_forecast
        ,plan_new_alc_sales_forecast as cw_plan_new_alc_sales_forecast
        ,sum(total_paid_orders) over ( order by day_of_week asc ) as cw_cumulative_total_paid_orders
        ,sum(total_paid_membership_orders) over ( order by day_of_week asc ) as cw_cumulative_total_paid_membership_orders
        ,sum(total_paid_alc_orders) over ( order by day_of_week asc ) as cw_cumulative_total_paid_alc_orders
        ,sum(existing_paid_ala_carte_orders) over ( order by day_of_week asc ) as cw_cumulative_existing_paid_ala_carte_orders
        ,sum(existing_paid_membership_orders) over ( order by day_of_week asc ) as cw_cumulative_existing_paid_membership_orders
        ,sum(new_paid_ala_carte_orders) over ( order by day_of_week asc ) as cw_cumulative_new_paid_ala_carte_orders
        ,sum(new_paid_membership_orders) over ( order by day_of_week asc ) as cw_cumulative_new_paid_membership_orders
        ,sum(total_paid_net_revenue) over ( order by day_of_week asc ) as cw_cumulative_total_paid_net_revenue
        ,sum(total_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as cw_cumulative_total_paid_ala_carte_net_revenue
        ,sum(total_paid_membership_net_revenue) over ( order by day_of_week asc ) as cw_cumulative_total_paid_membership_net_revenue
        ,sum(existing_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as cw_cumulative_existing_paid_ala_carte_net_revenue
        ,sum(existing_paid_membership_net_revenue) over ( order by day_of_week asc ) as cw_cumulative_existing_paid_membership_net_revenue
        ,sum(new_paid_ala_carte_net_revenue) over ( order by day_of_week asc ) as cw_cumulative_new_paid_ala_carte_net_revenue
        ,sum(new_paid_membership_net_revenue) over ( order by day_of_week asc ) as cw_cumulative_new_paid_membership_net_revenue
        ,sum(plan_total_orders) over ( order by day_of_week asc ) as cw_cumulative_plan_total_orders
        ,sum(plan_total_member_orders) over ( order by day_of_week asc ) as cw_cumulative_plan_total_member_orders
        ,sum(plan_total_alc_orders) over ( order by day_of_week asc ) as cw_cumulative_plan_total_alc_orders
        ,sum(plan_existing_member_orders) over ( order by day_of_week asc ) as cw_cumulative_plan_existing_member_orders
        ,sum(plan_existing_alc_orders) over ( order by day_of_week asc ) as cw_cumulative_plan_existing_alc_orders
        ,sum(plan_new_member_orders) over ( order by day_of_week asc ) as cw_cumulative_plan_new_member_orders
        ,sum(plan_new_alc_orders) over ( order by day_of_week asc ) as cw_cumulative_plan_new_alc_orders
        ,sum(plan_total_sales_forecast) over ( order by day_of_week asc ) as cw_cumulative_plan_total_sales_forecast
        ,sum(plan_total_memberships_sales_forecast) over ( order by day_of_week asc ) as cw_cumulative_plan_total_memberships_sales_forecast
        ,sum(plan_total_alc_sales_forecast) over ( order by day_of_week asc ) as cw_cumulative_plan_total_alc_sales_forecast
        ,sum(plan_existing_memberships_sales_forecast) over ( order by day_of_week asc ) as cw_cumulative_plan_existing_memberships_sales_forecast
        ,sum(plan_existing_alc_sales_forecast) over ( order by day_of_week asc ) as cw_cumulative_plan_existing_alc_sales_forecast
        ,sum(plan_new_memberships_sales_forecast) over ( order by day_of_week asc ) as cw_cumulative_plan_new_memberships_sales_forecast
        ,sum(plan_new_alc_sales_forecast) over ( order by day_of_week asc ) as cw_cumulative_plan_new_alc_sales_forecast
         
    from plan_performance
    where (((calendar_date) >= ((TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY)))) AND (calendar_date) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY))), INTERVAL 1 WEEK))))))
    
)

,performance_week_to_date as (
    select 
        DAY_OF_WEEK as wtd_day_of_week
        ,sum(total_paid_orders)  as wtd_cumulative_total_paid_orders
        ,sum(total_paid_membership_orders)  as wtd_cumulative_total_paid_membership_orders
        ,sum(total_paid_alc_orders)  as wtd_cumulative_total_paid_alc_orders
        ,sum(existing_paid_ala_carte_orders)  as wtd_cumulative_existing_paid_ala_carte_orders
        ,sum(existing_paid_membership_orders)  as wtd_cumulative_existing_paid_membership_orders
        ,sum(new_paid_ala_carte_orders)  as wtd_cumulative_new_paid_ala_carte_orders
        ,sum(new_paid_membership_orders)  as wtd_cumulative_new_paid_membership_orders
        ,sum(total_paid_net_revenue)  as wtd_cumulative_total_paid_net_revenue
        ,sum(total_paid_ala_carte_net_revenue)  as wtd_cumulative_total_paid_ala_carte_net_revenue
        ,sum(total_paid_membership_net_revenue)  as wtd_cumulative_total_paid_membership_net_revenue
        ,sum(existing_paid_ala_carte_net_revenue)  as wtd_cumulative_existing_paid_ala_carte_net_revenue
        ,sum(existing_paid_membership_net_revenue)  as wtd_cumulative_existing_paid_membership_net_revenue
        ,sum(new_paid_ala_carte_net_revenue)  as wtd_cumulative_new_paid_ala_carte_net_revenue
        ,sum(new_paid_membership_net_revenue)  as wtd_cumulative_new_paid_membership_net_revenue
        ,sum(plan_total_orders)  as wtd_cumulative_plan_total_orders
        ,sum(plan_total_member_orders)  as wtd_cumulative_plan_total_member_orders
        ,sum(plan_total_alc_orders)  as wtd_cumulative_plan_total_alc_orders
        ,sum(plan_existing_member_orders)  as wtd_cumulative_plan_existing_member_orders
        ,sum(plan_existing_alc_orders)  as wtd_cumulative_plan_existing_alc_orders
        ,sum(plan_new_member_orders)  as wtd_cumulative_plan_new_member_orders
        ,sum(plan_new_alc_orders)  as wtd_cumulative_plan_new_alc_orders
        ,sum(plan_total_sales_forecast)  as wtd_cumulative_plan_total_sales_forecast
        ,sum(plan_total_memberships_sales_forecast)  as wtd_cumulative_plan_total_memberships_sales_forecast
        ,sum(plan_total_alc_sales_forecast)  as wtd_cumulative_plan_total_alc_sales_forecast
        ,sum(plan_existing_memberships_sales_forecast)  as wtd_cumulative_plan_existing_memberships_sales_forecast
        ,sum(plan_existing_alc_sales_forecast)  as wtd_cumulative_plan_existing_alc_sales_forecast
        ,sum(plan_new_memberships_sales_forecast)  as wtd_cumulative_plan_new_memberships_sales_forecast
        ,sum(plan_new_alc_sales_forecast)  as wtd_cumulative_plan_new_alc_sales_forecast
    from plan_performance
    where (((calendar_date) >= ((TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY)))) AND (calendar_date) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), WEEK(SUNDAY))), INTERVAL 1 WEEK))))))
    group by 1 
    order by 1 asc
)


,performance_month_to_date as (
    select 
        DAY_OF_WEEK as mtd_day_of_week
        ,sum(total_paid_orders)  as mtd_cumulative_total_paid_orders
        ,sum(total_paid_membership_orders)  as mtd_cumulative_total_paid_membership_orders
        ,sum(total_paid_alc_orders)  as mtd_cumulative_total_paid_alc_orders
        ,sum(existing_paid_ala_carte_orders)  as mtd_cumulative_existing_paid_ala_carte_orders
        ,sum(existing_paid_membership_orders)  as mtd_cumulative_existing_paid_membership_orders
        ,sum(new_paid_ala_carte_orders)  as mtd_cumulative_new_paid_ala_carte_orders
        ,sum(new_paid_membership_orders)  as mtd_cumulative_new_paid_membership_orders
        ,sum(total_paid_net_revenue)  as mtd_cumulative_total_paid_net_revenue
        ,sum(total_paid_ala_carte_net_revenue)  as mtd_cumulative_total_paid_ala_carte_net_revenue
        ,sum(total_paid_membership_net_revenue)  as mtd_cumulative_total_paid_membership_net_revenue
        ,sum(existing_paid_ala_carte_net_revenue)  as mtd_cumulative_existing_paid_ala_carte_net_revenue
        ,sum(existing_paid_membership_net_revenue)  as mtd_cumulative_existing_paid_membership_net_revenue
        ,sum(new_paid_ala_carte_net_revenue)  as mtd_cumulative_new_paid_ala_carte_net_revenue
        ,sum(new_paid_membership_net_revenue)  as mtd_cumulative_new_paid_membership_net_revenue
        ,sum(plan_total_orders)  as mtd_cumulative_plan_total_orders
        ,sum(plan_total_member_orders)  as mtd_cumulative_plan_total_member_orders
        ,sum(plan_total_alc_orders)  as mtd_cumulative_plan_total_alc_orders
        ,sum(plan_existing_member_orders)  as mtd_cumulative_plan_existing_member_orders
        ,sum(plan_existing_alc_orders)  as mtd_cumulative_plan_existing_alc_orders
        ,sum(plan_new_member_orders)  as mtd_cumulative_plan_new_member_orders
        ,sum(plan_new_alc_orders)  as mtd_cumulative_plan_new_alc_orders
        ,sum(plan_total_sales_forecast)  as mtd_cumulative_plan_total_sales_forecast
        ,sum(plan_total_memberships_sales_forecast)  as mtd_cumulative_plan_total_memberships_sales_forecast
        ,sum(plan_total_alc_sales_forecast)  as mtd_cumulative_plan_total_alc_sales_forecast
        ,sum(plan_existing_memberships_sales_forecast)  as mtd_cumulative_plan_existing_memberships_sales_forecast
        ,sum(plan_existing_alc_sales_forecast)  as mtd_cumulative_plan_existing_alc_sales_forecast
        ,sum(plan_new_memberships_sales_forecast)  as mtd_cumulative_plan_new_memberships_sales_forecast
        ,sum(plan_new_alc_sales_forecast)  as mtd_cumulative_plan_new_alc_sales_forecast
         
    from plan_performance
    where ((( calendar_date  ) >= ((TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), MONTH))) AND ( calendar_date  ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), MONTH)), INTERVAL 1 MONTH))))))
    group by 1 
    order by 1 asc
)

,performance_quarter_to_date as (
 select 
        DAY_OF_WEEK as qtd_day_of_week
        ,sum(total_paid_orders)  as qtd_cumulative_total_paid_orders
        ,sum(total_paid_membership_orders)  as qtd_cumulative_total_paid_membership_orders
        ,sum(total_paid_alc_orders)  as qtd_cumulative_total_paid_alc_orders
        ,sum(existing_paid_ala_carte_orders)  as qtd_cumulative_existing_paid_ala_carte_orders
        ,sum(existing_paid_membership_orders)  as qtd_cumulative_existing_paid_membership_orders
        ,sum(new_paid_ala_carte_orders)  as qtd_cumulative_new_paid_ala_carte_orders
        ,sum(new_paid_membership_orders)  as qtd_cumulative_new_paid_membership_orders
        ,sum(total_paid_net_revenue)  as qtd_cumulative_total_paid_net_revenue
        ,sum(total_paid_ala_carte_net_revenue)  as qtd_cumulative_total_paid_ala_carte_net_revenue
        ,sum(total_paid_membership_net_revenue)  as qtd_cumulative_total_paid_membership_net_revenue
        ,sum(existing_paid_ala_carte_net_revenue)  as qtd_cumulative_existing_paid_ala_carte_net_revenue
        ,sum(existing_paid_membership_net_revenue)  as qtd_cumulative_existing_paid_membership_net_revenue
        ,sum(new_paid_ala_carte_net_revenue)  as qtd_cumulative_new_paid_ala_carte_net_revenue
        ,sum(new_paid_membership_net_revenue)  as qtd_cumulative_new_paid_membership_net_revenue
        ,sum(plan_total_orders)  as qtd_cumulative_plan_total_orders
        ,sum(plan_total_member_orders)  as qtd_cumulative_plan_total_member_orders
        ,sum(plan_total_alc_orders)  as qtd_cumulative_plan_total_alc_orders
        ,sum(plan_existing_member_orders)  as qtd_cumulative_plan_existing_member_orders
        ,sum(plan_existing_alc_orders)  as qtd_cumulative_plan_existing_alc_orders
        ,sum(plan_new_member_orders)  as qtd_cumulative_plan_new_member_orders
        ,sum(plan_new_alc_orders)  as qtd_cumulative_plan_new_alc_orders
        ,sum(plan_total_sales_forecast)  as qtd_cumulative_plan_total_sales_forecast
        ,sum(plan_total_memberships_sales_forecast)  as qtd_cumulative_plan_total_memberships_sales_forecast
        ,sum(plan_total_alc_sales_forecast)  as qtd_cumulative_plan_total_alc_sales_forecast
        ,sum(plan_existing_memberships_sales_forecast)  as qtd_cumulative_plan_existing_memberships_sales_forecast
        ,sum(plan_existing_alc_sales_forecast)  as qtd_cumulative_plan_existing_alc_sales_forecast
        ,sum(plan_new_memberships_sales_forecast)  as qtd_cumulative_plan_new_memberships_sales_forecast
        ,sum(plan_new_alc_sales_forecast)  as qtd_cumulative_plan_new_alc_sales_forecast
 from plan_performance
    where  ((( calendar_date  ) >= ((TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), QUARTER))) AND ( calendar_date  ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), QUARTER), QUARTER)), INTERVAL 3 MONTH))))))
    group by 1 
    order by 1 asc
)

--,performance_yesterday as (
 select 
    performance_current_week.*
    ,performance_previous_week.*
    ,performance_last_year.*
    ,performance_week_to_date.*
    ,performance_month_to_date.*
    ,performance_quarter_to_date.*

    -- Absolute variances
    ,cw_total_paid_orders - cw_plan_total_orders AS cw_diff_total_paid_orders
    ,cw_total_paid_membership_orders - cw_plan_total_member_orders AS cw_diff_paid_membership_orders
    ,cw_total_paid_alc_orders - cw_plan_total_alc_orders AS cw_diff_paid_alc_orders
    ,cw_existing_paid_ala_carte_orders - cw_plan_existing_alc_orders AS cw_diff_existing_ala_carte_orders
    ,cw_existing_paid_membership_orders - cw_plan_existing_member_orders AS cw_diff_existing_membership_orders
    ,cw_new_paid_ala_carte_orders - cw_plan_new_alc_orders AS cw_diff_new_ala_carte_orders
    ,cw_new_paid_membership_orders - cw_plan_new_member_orders AS cw_diff_new_membership_orders
    ,cw_total_paid_net_revenue - cw_plan_total_sales_forecast AS cw_diff_total_net_revenue
    ,cw_total_paid_ala_carte_net_revenue - cw_plan_total_alc_sales_forecast AS cw_diff_ala_carte_net_revenue
    ,cw_total_paid_membership_net_revenue - cw_plan_total_memberships_sales_forecast AS cw_diff_membership_net_revenue
    ,cw_existing_paid_ala_carte_net_revenue - cw_plan_existing_alc_sales_forecast AS cw_diff_existing_ala_carte_revenue
    ,cw_existing_paid_membership_net_revenue - cw_plan_existing_memberships_sales_forecast AS cw_diff_existing_membership_revenue
    ,cw_new_paid_ala_carte_net_revenue - cw_plan_new_alc_sales_forecast AS cw_diff_new_ala_carte_revenue
    ,cw_new_paid_membership_net_revenue - cw_plan_new_memberships_sales_forecast AS cw_diff_new_membership_revenue

    -- Percent-to-plan variances
    ,SAFE_DIVIDE((cw_total_paid_orders    - cw_plan_total_orders),             cw_plan_total_orders)             * 100 AS cw_pct_diff_total_paid_orders
    ,SAFE_DIVIDE((cw_total_paid_membership_orders - cw_plan_total_member_orders), cw_plan_total_member_orders) * 100 AS cw_pct_diff_paid_membership_orders
    ,SAFE_DIVIDE((cw_total_paid_alc_orders   - cw_plan_total_alc_orders),        cw_plan_total_alc_orders)        * 100 AS cw_pct_diff_paid_alc_orders
    ,SAFE_DIVIDE((cw_existing_paid_ala_carte_orders - cw_plan_existing_alc_orders), cw_plan_existing_alc_orders) * 100 AS cw_pct_diff_existing_ala_carte_orders
    ,SAFE_DIVIDE((cw_existing_paid_membership_orders - cw_plan_existing_member_orders), cw_plan_existing_member_orders) * 100 AS cw_pct_diff_existing_membership_orders
    ,SAFE_DIVIDE((cw_new_paid_ala_carte_orders - cw_plan_new_alc_orders),       cw_plan_new_alc_orders)           * 100 AS cw_pct_diff_new_ala_carte_orders
    ,SAFE_DIVIDE((cw_new_paid_membership_orders - cw_plan_new_member_orders),   cw_plan_new_member_orders)        * 100 AS cw_pct_diff_new_membership_orders

    ,SAFE_DIVIDE((cw_total_paid_net_revenue          - cw_plan_total_sales_forecast),      cw_plan_total_sales_forecast)      * 100 AS cw_pct_diff_total_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_ala_carte_net_revenue - cw_plan_total_alc_sales_forecast), cw_plan_total_alc_sales_forecast) * 100 AS cw_pct_diff_ala_carte_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_membership_net_revenue - cw_plan_total_memberships_sales_forecast), cw_plan_total_memberships_sales_forecast) * 100 AS cw_pct_diff_membership_net_revenue
    ,SAFE_DIVIDE((cw_existing_paid_ala_carte_net_revenue - cw_plan_existing_alc_sales_forecast), cw_plan_existing_alc_sales_forecast) * 100 AS cw_pct_diff_existing_ala_carte_revenue
    ,SAFE_DIVIDE((cw_existing_paid_membership_net_revenue - cw_plan_existing_memberships_sales_forecast), cw_plan_existing_memberships_sales_forecast) * 100 AS cw_pct_diff_existing_membership_revenue
    ,SAFE_DIVIDE((cw_new_paid_ala_carte_net_revenue   - cw_plan_new_alc_sales_forecast),       cw_plan_new_alc_sales_forecast)       * 100 AS cw_pct_diff_new_ala_carte_revenue
    ,SAFE_DIVIDE((cw_new_paid_membership_net_revenue - cw_plan_new_memberships_sales_forecast),   cw_plan_new_memberships_sales_forecast) * 100 AS cw_pct_diff_new_membership_revenue

        -- Current week vs Previous week variances
    ,cw_total_paid_orders - pw_total_paid_orders AS cw_vs_pw_diff_total_paid_orders
    ,SAFE_DIVIDE((cw_total_paid_orders - pw_total_paid_orders), pw_total_paid_orders) * 100 AS cw_vs_pw_pct_diff_total_paid_orders
    ,cw_total_paid_membership_orders - pw_total_paid_membership_orders AS cw_vs_pw_diff_paid_membership_orders
    ,SAFE_DIVIDE((cw_total_paid_membership_orders - pw_total_paid_membership_orders), pw_total_paid_membership_orders) * 100 AS cw_vs_pw_pct_diff_paid_membership_orders
    ,cw_total_paid_alc_orders - pw_total_paid_alc_orders AS cw_vs_pw_diff_paid_alc_orders
    ,SAFE_DIVIDE((cw_total_paid_alc_orders - pw_total_paid_alc_orders), pw_total_paid_alc_orders) * 100 AS cw_vs_pw_pct_diff_paid_alc_orders
    ,cw_existing_paid_ala_carte_orders - pw_existing_paid_ala_carte_orders AS cw_vs_pw_diff_existing_ala_carte_orders
    ,SAFE_DIVIDE((cw_existing_paid_ala_carte_orders - pw_existing_paid_ala_carte_orders), pw_existing_paid_ala_carte_orders) * 100 AS cw_vs_pw_pct_diff_existing_ala_carte_orders
    ,cw_existing_paid_membership_orders - pw_existing_paid_membership_orders AS cw_vs_pw_diff_existing_membership_orders
    ,SAFE_DIVIDE((cw_existing_paid_membership_orders - pw_existing_paid_membership_orders), pw_existing_paid_membership_orders) * 100 AS cw_vs_pw_pct_diff_existing_membership_orders
    ,cw_new_paid_ala_carte_orders - pw_new_paid_ala_carte_orders AS cw_vs_pw_diff_new_ala_carte_orders
    ,SAFE_DIVIDE((cw_new_paid_ala_carte_orders - pw_new_paid_ala_carte_orders), pw_new_paid_ala_carte_orders) * 100 AS cw_vs_pw_pct_diff_new_ala_carte_orders
    ,cw_new_paid_membership_orders - pw_new_paid_membership_orders AS cw_vs_pw_diff_new_membership_orders
    ,SAFE_DIVIDE((cw_new_paid_membership_orders - pw_new_paid_membership_orders), pw_new_paid_membership_orders) * 100 AS cw_vs_pw_pct_diff_new_membership_orders

    ,cw_total_paid_net_revenue - pw_total_paid_net_revenue AS cw_vs_pw_diff_total_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_net_revenue - pw_total_paid_net_revenue), pw_total_paid_net_revenue) * 100 AS cw_vs_pw_pct_diff_total_net_revenue
    ,cw_total_paid_ala_carte_net_revenue - pw_total_paid_ala_carte_net_revenue AS cw_vs_pw_diff_ala_carte_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_ala_carte_net_revenue - pw_total_paid_ala_carte_net_revenue), pw_total_paid_ala_carte_net_revenue) * 100 AS cw_vs_pw_pct_diff_ala_carte_net_revenue
    ,cw_total_paid_membership_net_revenue - pw_total_paid_membership_net_revenue AS cw_vs_pw_diff_membership_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_membership_net_revenue - pw_total_paid_membership_net_revenue), pw_total_paid_membership_net_revenue) * 100 AS cw_vs_pw_pct_diff_membership_net_revenue
    ,cw_existing_paid_ala_carte_net_revenue - pw_existing_paid_ala_carte_net_revenue AS cw_vs_pw_diff_existing_ala_carte_revenue
    ,SAFE_DIVIDE((cw_existing_paid_ala_carte_net_revenue - pw_existing_paid_ala_carte_net_revenue), pw_existing_paid_ala_carte_net_revenue) * 100 AS cw_vs_pw_pct_diff_existing_ala_carte_revenue
    ,cw_existing_paid_membership_net_revenue - pw_existing_paid_membership_net_revenue AS cw_vs_pw_diff_existing_membership_revenue
    ,SAFE_DIVIDE((cw_existing_paid_membership_net_revenue - pw_existing_paid_membership_net_revenue), pw_existing_paid_membership_net_revenue) * 100 AS cw_vs_pw_pct_diff_existing_membership_revenue
    ,cw_new_paid_ala_carte_net_revenue - pw_new_paid_ala_carte_net_revenue AS cw_vs_pw_diff_new_ala_carte_revenue
    ,SAFE_DIVIDE((cw_new_paid_ala_carte_net_revenue - pw_new_paid_ala_carte_net_revenue), pw_new_paid_ala_carte_net_revenue) * 100 AS cw_vs_pw_pct_diff_new_ala_carte_revenue
    ,cw_new_paid_membership_net_revenue - pw_new_paid_membership_net_revenue AS cw_vs_pw_diff_new_membership_revenue
    ,SAFE_DIVIDE((cw_new_paid_membership_net_revenue - pw_new_paid_membership_net_revenue), pw_new_paid_membership_net_revenue) * 100 AS cw_vs_pw_pct_diff_new_membership_revenue

    -- Current week vs Last year variances
    ,cw_total_paid_orders - ly_total_paid_orders AS cw_vs_ly_diff_total_paid_orders
    ,SAFE_DIVIDE((cw_total_paid_orders - ly_total_paid_orders), ly_total_paid_orders) * 100 AS cw_vs_ly_pct_diff_total_paid_orders
    ,cw_total_paid_membership_orders - ly_total_paid_membership_orders AS cw_vs_ly_diff_paid_membership_orders
    ,SAFE_DIVIDE((cw_total_paid_membership_orders - ly_total_paid_membership_orders), ly_total_paid_membership_orders) * 100 AS cw_vs_ly_pct_diff_paid_membership_orders
    ,cw_total_paid_alc_orders - ly_total_paid_alc_orders AS cw_vs_ly_diff_paid_alc_orders
    ,SAFE_DIVIDE((cw_total_paid_alc_orders - ly_total_paid_alc_orders), ly_total_paid_alc_orders) * 100 AS cw_vs_ly_pct_diff_paid_alc_orders
    ,cw_existing_paid_ala_carte_orders - ly_existing_paid_ala_carte_orders AS cw_vs_ly_diff_existing_ala_carte_orders
    ,SAFE_DIVIDE((cw_existing_paid_ala_carte_orders - ly_existing_paid_ala_carte_orders), ly_existing_paid_ala_carte_orders) * 100 AS cw_vs_ly_pct_diff_existing_ala_carte_orders
    ,cw_existing_paid_membership_orders - ly_existing_paid_membership_orders AS cw_vs_ly_diff_existing_membership_orders
    ,SAFE_DIVIDE((cw_existing_paid_membership_orders - ly_existing_paid_membership_orders), ly_existing_paid_membership_orders) * 100 AS cw_vs_ly_pct_diff_existing_membership_orders
    ,cw_new_paid_ala_carte_orders - ly_new_paid_ala_carte_orders AS cw_vs_ly_diff_new_ala_carte_orders
    ,SAFE_DIVIDE((cw_new_paid_ala_carte_orders - ly_new_paid_ala_carte_orders), ly_new_paid_ala_carte_orders) * 100 AS cw_vs_ly_pct_diff_new_ala_carte_orders
    ,cw_new_paid_membership_orders - ly_new_paid_membership_orders AS cw_vs_ly_diff_new_membership_orders
    ,SAFE_DIVIDE((cw_new_paid_membership_orders - ly_new_paid_membership_orders), ly_new_paid_membership_orders) * 100 AS cw_vs_ly_pct_diff_new_membership_orders

    ,cw_total_paid_net_revenue - ly_total_paid_net_revenue AS cw_vs_ly_diff_total_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_net_revenue - ly_total_paid_net_revenue), ly_total_paid_net_revenue) * 100 AS cw_vs_ly_pct_diff_total_net_revenue
    ,cw_total_paid_ala_carte_net_revenue - ly_total_paid_ala_carte_net_revenue AS cw_vs_ly_diff_ala_carte_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_ala_carte_net_revenue - ly_total_paid_ala_carte_net_revenue), ly_total_paid_ala_carte_net_revenue) * 100 AS cw_vs_ly_pct_diff_ala_carte_net_revenue
    ,cw_total_paid_membership_net_revenue - ly_total_paid_membership_net_revenue AS cw_vs_ly_diff_membership_net_revenue
    ,SAFE_DIVIDE((cw_total_paid_membership_net_revenue - ly_total_paid_membership_net_revenue), ly_total_paid_membership_net_revenue) * 100 AS cw_vs_ly_pct_diff_membership_net_revenue
    ,cw_existing_paid_ala_carte_net_revenue - ly_existing_paid_ala_carte_net_revenue AS cw_vs_ly_diff_existing_ala_carte_revenue
    ,SAFE_DIVIDE((cw_existing_paid_ala_carte_net_revenue - ly_existing_paid_ala_carte_net_revenue), ly_existing_paid_ala_carte_net_revenue) * 100 AS cw_vs_ly_pct_diff_existing_ala_carte_revenue
    ,cw_existing_paid_membership_net_revenue - ly_existing_paid_membership_net_revenue AS cw_vs_ly_diff_existing_membership_revenue
    ,SAFE_DIVIDE((cw_existing_paid_membership_net_revenue - ly_existing_paid_membership_net_revenue), ly_existing_paid_membership_net_revenue) * 100 AS cw_vs_ly_pct_diff_existing_membership_revenue
    ,cw_new_paid_ala_carte_net_revenue - ly_new_paid_ala_carte_net_revenue AS cw_vs_ly_diff_new_ala_carte_revenue
    ,SAFE_DIVIDE((cw_new_paid_ala_carte_net_revenue - ly_new_paid_ala_carte_net_revenue), ly_new_paid_ala_carte_net_revenue) * 100 AS cw_vs_ly_pct_diff_new_ala_carte_revenue
    ,cw_new_paid_membership_net_revenue - ly_new_paid_membership_net_revenue AS cw_vs_ly_diff_new_membership_revenue
    ,SAFE_DIVIDE((cw_new_paid_membership_net_revenue - ly_new_paid_membership_net_revenue), ly_new_paid_membership_net_revenue) * 100 AS cw_vs_ly_pct_diff_new_membership_revenue

        -- WTD actual vs Plan variances
    ,cw_cumulative_total_paid_orders        - cw_cumulative_plan_total_orders            AS wtd_diff_total_paid_orders
    ,SAFE_DIVIDE((cw_cumulative_total_paid_orders - cw_cumulative_plan_total_orders), cw_cumulative_plan_total_orders) * 100 AS wtd_pct_diff_total_paid_orders
    ,cw_cumulative_total_paid_membership_orders - cw_cumulative_plan_total_member_orders    AS wtd_diff_paid_membership_orders
    ,SAFE_DIVIDE((cw_cumulative_total_paid_membership_orders - cw_cumulative_plan_total_member_orders), cw_cumulative_plan_total_member_orders) * 100 AS wtd_pct_diff_paid_membership_orders
    ,cw_cumulative_total_paid_alc_orders  - cw_cumulative_plan_total_alc_orders          AS wtd_diff_paid_alc_orders
    ,SAFE_DIVIDE((cw_cumulative_total_paid_alc_orders - cw_cumulative_plan_total_alc_orders), cw_cumulative_plan_total_alc_orders) * 100 AS wtd_pct_diff_paid_alc_orders
    ,cw_cumulative_existing_paid_ala_carte_orders - cw_cumulative_plan_existing_alc_orders  AS wtd_diff_existing_ala_carte_orders
    ,SAFE_DIVIDE((cw_cumulative_existing_paid_ala_carte_orders - cw_cumulative_plan_existing_alc_orders), cw_cumulative_plan_existing_alc_orders) * 100 AS wtd_pct_diff_existing_ala_carte_orders
    ,cw_cumulative_existing_paid_membership_orders - cw_cumulative_plan_existing_member_orders AS wtd_diff_existing_membership_orders
    ,SAFE_DIVIDE((cw_cumulative_existing_paid_membership_orders - cw_cumulative_plan_existing_member_orders), cw_cumulative_plan_existing_member_orders) * 100 AS wtd_pct_diff_existing_membership_orders
    ,cw_cumulative_new_paid_ala_carte_orders - cw_cumulative_plan_new_alc_orders           AS wtd_diff_new_ala_carte_orders
    ,SAFE_DIVIDE((cw_cumulative_new_paid_ala_carte_orders - cw_cumulative_plan_new_alc_orders), cw_cumulative_plan_new_alc_orders) * 100 AS wtd_pct_diff_new_ala_carte_orders
    ,cw_cumulative_new_paid_membership_orders - cw_cumulative_plan_new_member_orders        AS wtd_diff_new_membership_orders
    ,SAFE_DIVIDE((cw_cumulative_new_paid_membership_orders - cw_cumulative_plan_new_member_orders), cw_cumulative_plan_new_member_orders) * 100 AS wtd_pct_diff_new_membership_orders

    ,cw_cumulative_total_paid_net_revenue   - cw_cumulative_plan_total_sales_forecast    AS wtd_diff_total_net_revenue
    ,SAFE_DIVIDE((cw_cumulative_total_paid_net_revenue - cw_cumulative_plan_total_sales_forecast), cw_cumulative_plan_total_sales_forecast) * 100 AS wtd_pct_diff_total_net_revenue
    ,cw_cumulative_total_paid_ala_carte_net_revenue - cw_cumulative_plan_total_alc_sales_forecast   AS wtd_diff_ala_carte_net_revenue
    ,SAFE_DIVIDE((cw_cumulative_total_paid_ala_carte_net_revenue - cw_cumulative_plan_total_alc_sales_forecast), cw_cumulative_plan_total_alc_sales_forecast) * 100 AS wtd_pct_diff_ala_carte_net_revenue
    ,cw_cumulative_total_paid_membership_net_revenue - cw_cumulative_plan_total_memberships_sales_forecast AS wtd_diff_membership_net_revenue
    ,SAFE_DIVIDE((cw_cumulative_total_paid_membership_net_revenue - cw_cumulative_plan_total_memberships_sales_forecast), cw_cumulative_plan_total_memberships_sales_forecast) * 100 AS wtd_pct_diff_membership_net_revenue
    ,cw_cumulative_existing_paid_ala_carte_net_revenue - cw_cumulative_plan_existing_alc_sales_forecast  AS wtd_diff_existing_ala_carte_revenue
    ,SAFE_DIVIDE((cw_cumulative_existing_paid_ala_carte_net_revenue - cw_cumulative_plan_existing_alc_sales_forecast), cw_cumulative_plan_existing_alc_sales_forecast) * 100 AS wtd_pct_diff_existing_ala_carte_revenue
    ,cw_cumulative_existing_paid_membership_net_revenue - cw_cumulative_plan_existing_memberships_sales_forecast AS wtd_diff_existing_membership_revenue
    ,SAFE_DIVIDE((cw_cumulative_existing_paid_membership_net_revenue - cw_cumulative_plan_existing_memberships_sales_forecast), cw_cumulative_plan_existing_memberships_sales_forecast) * 100 AS wtd_pct_diff_existing_membership_revenue
    ,cw_cumulative_new_paid_ala_carte_net_revenue - cw_cumulative_plan_new_alc_sales_forecast          AS wtd_diff_new_ala_carte_revenue
    ,SAFE_DIVIDE((cw_cumulative_new_paid_ala_carte_net_revenue - cw_cumulative_plan_new_alc_sales_forecast), cw_cumulative_plan_new_alc_sales_forecast) * 100 AS wtd_pct_diff_new_ala_carte_revenue
    ,cw_cumulative_new_paid_membership_net_revenue - cw_cumulative_plan_new_memberships_sales_forecast  AS wtd_diff_new_membership_revenue
    ,SAFE_DIVIDE((cw_cumulative_new_paid_membership_net_revenue - cw_cumulative_plan_new_memberships_sales_forecast), cw_cumulative_plan_new_memberships_sales_forecast) * 100 AS wtd_pct_diff_new_membership_revenue


        -- MTD actual vs Plan variances
    ,mtd_cumulative_total_paid_orders        - mtd_cumulative_plan_total_orders            AS mtd_diff_total_paid_orders
    ,SAFE_DIVIDE((mtd_cumulative_total_paid_orders - mtd_cumulative_plan_total_orders), mtd_cumulative_plan_total_orders) * 100 AS mtd_pct_diff_total_paid_orders
    ,mtd_cumulative_total_paid_membership_orders - mtd_cumulative_plan_total_member_orders    AS mtd_diff_paid_membership_orders
    ,SAFE_DIVIDE((mtd_cumulative_total_paid_membership_orders - mtd_cumulative_plan_total_member_orders), mtd_cumulative_plan_total_member_orders) * 100 AS mtd_pct_diff_paid_membership_orders
    ,mtd_cumulative_total_paid_alc_orders  - mtd_cumulative_plan_total_alc_orders          AS mtd_diff_paid_alc_orders
    ,SAFE_DIVIDE((mtd_cumulative_total_paid_alc_orders - mtd_cumulative_plan_total_alc_orders), mtd_cumulative_plan_total_alc_orders) * 100 AS mtd_pct_diff_paid_alc_orders
    ,mtd_cumulative_existing_paid_ala_carte_orders - mtd_cumulative_plan_existing_alc_orders  AS mtd_diff_existing_ala_carte_orders
    ,SAFE_DIVIDE((mtd_cumulative_existing_paid_ala_carte_orders - mtd_cumulative_plan_existing_alc_orders), mtd_cumulative_plan_existing_alc_orders) * 100 AS mtd_pct_diff_existing_ala_carte_orders
    ,mtd_cumulative_existing_paid_membership_orders - mtd_cumulative_plan_existing_member_orders AS mtd_diff_existing_membership_orders
    ,SAFE_DIVIDE((mtd_cumulative_existing_paid_membership_orders - mtd_cumulative_plan_existing_member_orders), mtd_cumulative_plan_existing_member_orders) * 100 AS mtd_pct_diff_existing_membership_orders
    ,mtd_cumulative_new_paid_ala_carte_orders - mtd_cumulative_plan_new_alc_orders           AS mtd_diff_new_ala_carte_orders
    ,SAFE_DIVIDE((mtd_cumulative_new_paid_ala_carte_orders - mtd_cumulative_plan_new_alc_orders), mtd_cumulative_plan_new_alc_orders) * 100 AS mtd_pct_diff_new_ala_carte_orders
    ,mtd_cumulative_new_paid_membership_orders - mtd_cumulative_plan_new_member_orders        AS mtd_diff_new_membership_orders
    ,SAFE_DIVIDE((mtd_cumulative_new_paid_membership_orders - mtd_cumulative_plan_new_member_orders), mtd_cumulative_plan_new_member_orders) * 100 AS mtd_pct_diff_new_membership_orders

    ,mtd_cumulative_total_paid_net_revenue   - mtd_cumulative_plan_total_sales_forecast    AS mtd_diff_total_net_revenue
    ,SAFE_DIVIDE((mtd_cumulative_total_paid_net_revenue - mtd_cumulative_plan_total_sales_forecast), mtd_cumulative_plan_total_sales_forecast) * 100 AS mtd_pct_diff_total_net_revenue
    ,mtd_cumulative_total_paid_ala_carte_net_revenue - mtd_cumulative_plan_total_alc_sales_forecast   AS mtd_diff_ala_carte_net_revenue
    ,SAFE_DIVIDE((mtd_cumulative_total_paid_ala_carte_net_revenue - mtd_cumulative_plan_total_alc_sales_forecast), mtd_cumulative_plan_total_alc_sales_forecast) * 100 AS mtd_pct_diff_ala_carte_net_revenue
    ,mtd_cumulative_total_paid_membership_net_revenue - mtd_cumulative_plan_total_memberships_sales_forecast AS mtd_diff_membership_net_revenue
    ,SAFE_DIVIDE((mtd_cumulative_total_paid_membership_net_revenue - mtd_cumulative_plan_total_memberships_sales_forecast), mtd_cumulative_plan_total_memberships_sales_forecast) * 100 AS mtd_pct_diff_membership_net_revenue
    ,mtd_cumulative_existing_paid_ala_carte_net_revenue - mtd_cumulative_plan_existing_alc_sales_forecast  AS mtd_diff_existing_ala_carte_revenue
    ,SAFE_DIVIDE((mtd_cumulative_existing_paid_ala_carte_net_revenue - mtd_cumulative_plan_existing_alc_sales_forecast), mtd_cumulative_plan_existing_alc_sales_forecast) * 100 AS mtd_pct_diff_existing_ala_carte_revenue
    ,mtd_cumulative_existing_paid_membership_net_revenue - mtd_cumulative_plan_existing_memberships_sales_forecast AS mtd_diff_existing_membership_revenue
    ,SAFE_DIVIDE((mtd_cumulative_existing_paid_membership_net_revenue - mtd_cumulative_plan_existing_memberships_sales_forecast), mtd_cumulative_plan_existing_memberships_sales_forecast) * 100 AS mtd_pct_diff_existing_membership_revenue
    ,mtd_cumulative_new_paid_ala_carte_net_revenue - mtd_cumulative_plan_new_alc_sales_forecast          AS mtd_diff_new_ala_carte_revenue
    ,SAFE_DIVIDE((mtd_cumulative_new_paid_ala_carte_net_revenue - mtd_cumulative_plan_new_alc_sales_forecast), mtd_cumulative_plan_new_alc_sales_forecast) * 100 AS mtd_pct_diff_new_ala_carte_revenue
    ,mtd_cumulative_new_paid_membership_net_revenue - mtd_cumulative_plan_new_memberships_sales_forecast  AS mtd_diff_new_membership_revenue
    ,SAFE_DIVIDE((mtd_cumulative_new_paid_membership_net_revenue - mtd_cumulative_plan_new_memberships_sales_forecast), mtd_cumulative_plan_new_memberships_sales_forecast) * 100 AS mtd_pct_diff_new_membership_revenue

    -- QTD actual vs Plan variances
    ,qtd_cumulative_total_paid_orders        - qtd_cumulative_plan_total_orders            AS qtd_diff_total_paid_orders
    ,SAFE_DIVIDE((qtd_cumulative_total_paid_orders - qtd_cumulative_plan_total_orders), qtd_cumulative_plan_total_orders) * 100 AS qtd_pct_diff_total_paid_orders
    ,qtd_cumulative_total_paid_membership_orders - qtd_cumulative_plan_total_member_orders    AS qtd_diff_paid_membership_orders
    ,SAFE_DIVIDE((qtd_cumulative_total_paid_membership_orders - qtd_cumulative_plan_total_member_orders), qtd_cumulative_plan_total_member_orders) * 100 AS qtd_pct_diff_paid_membership_orders
    ,qtd_cumulative_total_paid_alc_orders  - qtd_cumulative_plan_total_alc_orders          AS qtd_diff_paid_alc_orders
    ,SAFE_DIVIDE((qtd_cumulative_total_paid_alc_orders - qtd_cumulative_plan_total_alc_orders), qtd_cumulative_plan_total_alc_orders) * 100 AS qtd_pct_diff_paid_alc_orders
    ,qtd_cumulative_existing_paid_ala_carte_orders - qtd_cumulative_plan_existing_alc_orders  AS qtd_diff_existing_ala_carte_orders
    ,SAFE_DIVIDE((qtd_cumulative_existing_paid_ala_carte_orders - qtd_cumulative_plan_existing_alc_orders), qtd_cumulative_plan_existing_alc_orders) * 100 AS qtd_pct_diff_existing_ala_carte_orders
    ,qtd_cumulative_existing_paid_membership_orders - qtd_cumulative_plan_existing_member_orders AS qtd_diff_existing_membership_orders
    ,SAFE_DIVIDE((qtd_cumulative_existing_paid_membership_orders - qtd_cumulative_plan_existing_member_orders), qtd_cumulative_plan_existing_member_orders) * 100 AS qtd_pct_diff_existing_membership_orders
    ,qtd_cumulative_new_paid_ala_carte_orders - qtd_cumulative_plan_new_alc_orders           AS qtd_diff_new_ala_carte_orders
    ,SAFE_DIVIDE((qtd_cumulative_new_paid_ala_carte_orders - qtd_cumulative_plan_new_alc_orders), qtd_cumulative_plan_new_alc_orders) * 100 AS qtd_pct_diff_new_ala_carte_orders
    ,qtd_cumulative_new_paid_membership_orders - qtd_cumulative_plan_new_member_orders        AS qtd_diff_new_membership_orders
    ,SAFE_DIVIDE((qtd_cumulative_new_paid_membership_orders - qtd_cumulative_plan_new_member_orders), qtd_cumulative_plan_new_member_orders) * 100 AS qtd_pct_diff_new_membership_orders
    ,qtd_cumulative_total_paid_net_revenue   - qtd_cumulative_plan_total_sales_forecast    AS qtd_diff_total_net_revenue
    ,SAFE_DIVIDE((qtd_cumulative_total_paid_net_revenue - qtd_cumulative_plan_total_sales_forecast), qtd_cumulative_plan_total_sales_forecast) * 100 AS qtd_pct_diff_total_net_revenue
    ,qtd_cumulative_total_paid_ala_carte_net_revenue - qtd_cumulative_plan_total_alc_sales_forecast   AS qtd_diff_ala_carte_net_revenue
    ,SAFE_DIVIDE((qtd_cumulative_total_paid_ala_carte_net_revenue - qtd_cumulative_plan_total_alc_sales_forecast), qtd_cumulative_plan_total_alc_sales_forecast) * 100 AS qtd_pct_diff_ala_carte_net_revenue
    ,qtd_cumulative_total_paid_membership_net_revenue - qtd_cumulative_plan_total_memberships_sales_forecast AS qtd_diff_membership_net_revenue
    ,SAFE_DIVIDE((qtd_cumulative_total_paid_membership_net_revenue - qtd_cumulative_plan_total_memberships_sales_forecast), qtd_cumulative_plan_total_memberships_sales_forecast) * 100 AS qtd_pct_diff_membership_net_revenue
    ,qtd_cumulative_existing_paid_ala_carte_net_revenue - qtd_cumulative_plan_existing_alc_sales_forecast  AS qtd_diff_existing_ala_carte_revenue
    ,SAFE_DIVIDE((qtd_cumulative_existing_paid_ala_carte_net_revenue - qtd_cumulative_plan_existing_alc_sales_forecast), qtd_cumulative_plan_existing_alc_sales_forecast) * 100 AS qtd_pct_diff_existing_ala_carte_revenue
    ,qtd_cumulative_existing_paid_membership_net_revenue - qtd_cumulative_plan_existing_memberships_sales_forecast AS qtd_diff_existing_membership_revenue
    ,SAFE_DIVIDE((qtd_cumulative_existing_paid_membership_net_revenue - qtd_cumulative_plan_existing_memberships_sales_forecast), qtd_cumulative_plan_existing_memberships_sales_forecast) * 100 AS qtd_pct_diff_existing_membership_revenue
    ,qtd_cumulative_new_paid_ala_carte_net_revenue - qtd_cumulative_plan_new_alc_sales_forecast          AS qtd_diff_new_ala_carte_revenue
    ,SAFE_DIVIDE((qtd_cumulative_new_paid_ala_carte_net_revenue - qtd_cumulative_plan_new_alc_sales_forecast), qtd_cumulative_plan_new_alc_sales_forecast) * 100 AS qtd_pct_diff_new_ala_carte_revenue
    ,qtd_cumulative_new_paid_membership_net_revenue - qtd_cumulative_plan_new_memberships_sales_forecast  AS qtd_diff_new_membership_revenue
    ,SAFE_DIVIDE((qtd_cumulative_new_paid_membership_net_revenue - qtd_cumulative_plan_new_memberships_sales_forecast), qtd_cumulative_plan_new_memberships_sales_forecast) * 100 AS qtd_pct_diff_new_membership_revenue


 from performance_current_week
 left join performance_previous_week on performance_previous_week.pw_day_of_week = performance_current_week.cw_day_of_week
 left join performance_last_year on performance_last_year.ly_day_of_week = performance_current_week.cw_day_of_week
 left join performance_week_to_date on performance_week_to_date.wtd_day_of_week = performance_current_week.cw_day_of_week
 left join performance_month_to_date on performance_month_to_date.mtd_day_of_week = performance_current_week.cw_day_of_week
 left join performance_quarter_to_date on performance_quarter_to_date.qtd_day_of_week = performance_current_week.cw_day_of_week


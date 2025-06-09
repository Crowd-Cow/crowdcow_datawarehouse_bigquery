with fiscal_calendar as (select * from {{ ref('retail_calendar') }} where fiscal_year > 2022) 
,filtered_orders as (select * from {{ ref('orders') }} 
        where order_type in ('E-COMMERCE', 'CC')
         AND (NOT is_rastellis OR is_rastellis IS NULL)
         AND (NOT is_qvc OR is_qvc IS NULL)
         AND (NOT is_seabear OR is_seabear IS NULL)
         AND (NOT is_backyard_butchers OR is_backyard_butchers IS NULL)
         AND is_paid_order
         AND not is_cancelled_order
         AND cast(order_paid_at_utc as date) >= '2022-01-01'  )

,user_membership as (select * from {{ ref('int_user_memberships') }})
,memberships as (select * from {{ ref('memberships') }})
,weekly_calendar AS (
          SELECT
              fiscal_year,
              fiscal_week_num,
              timestamp(calendar_date) as calendar_date,
              timestamp(date(fc.calendar_date_week_sun)) AS week_start_timestamp,
              timestamp(DATE_ADD(fc.calendar_date_week_sun, INTERVAL 7 DAY)) AS week_end_timestamp,
          FROM fiscal_calendar fc
          WHERE calendar_date <= CURRENT_DATE()
)
,daily_calendar AS (
    SELECT
        fiscal_year,
        fiscal_week_num,
        TIMESTAMP(calendar_date) AS calendar_date,
        TIMESTAMP(calendar_date) AS day_start_timestamp,
        TIMESTAMP(DATE_ADD(calendar_date, INTERVAL 24 HOUR)) AS day_end_timestamp
    FROM fiscal_calendar
    WHERE calendar_date <= CURRENT_DATE()
)
,weekly_final as (
    SELECT
    c.fiscal_year,
    c.fiscal_week_num,
    week_start_timestamp as period_start,
    week_end_timestamp as period_end,
    o.user_id,
    ------------------------ Active subs 180 days -----------------
    countif(memberships.user_id is not null and subscription_created_at_utc < week_start_timestamp
               and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > week_start_timestamp)
               and order_paid_at_utc >= TIMESTAMP_SUB(week_start_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < week_start_timestamp
               and is_membership_order ) as week_start_active_subscribers_180,
    countif(memberships.user_id is not null and subscription_created_at_utc < week_end_timestamp
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > week_end_timestamp)
                and order_paid_at_utc >= TIMESTAMP_SUB(week_end_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < week_end_timestamp
                and is_membership_order ) as week_end_active_subscribers_180,

    ------------------------ Active subs 90 days -----------------                
    countif(memberships.user_id is not null and subscription_created_at_utc < week_start_timestamp
               and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > week_start_timestamp)
               and order_paid_at_utc >= TIMESTAMP_SUB(week_start_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < week_start_timestamp
               and is_membership_order ) as week_start_active_subscribers_90,

    countif(memberships.user_id is not null and subscription_created_at_utc < week_end_timestamp
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > week_end_timestamp)
                and order_paid_at_utc >= TIMESTAMP_SUB(week_end_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < week_end_timestamp
                and is_membership_order ) as week_end_active_subscribers_90,

    countif(memberships.user_id is not null and (subscription_created_at_utc >= week_start_timestamp and subscription_created_at_utc <= week_end_timestamp)
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > week_start_timestamp)
                AND o.paid_order_rank = 1 AND o.paid_unique_membership_order_rank = 1
                and order_paid_at_utc >= week_start_timestamp and order_paid_at_utc <= week_end_timestamp
                and is_membership_order) as new_first_subscriber,

    countif(memberships.user_id is not null and (subscription_created_at_utc >= week_start_timestamp and subscription_created_at_utc <= week_end_timestamp)
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > week_start_timestamp)
                AND o.paid_order_rank > 1 AND o.paid_unique_membership_order_rank = 1
                and order_paid_at_utc >= week_start_timestamp and order_paid_at_utc <= week_end_timestamp
                and is_membership_order) as new_subscriber,

    countif(memberships.user_id is not null and subscription_created_at_utc < week_start_timestamp
                and subscription_cancelled_at_utc >= week_start_timestamp and subscription_cancelled_at_utc <= week_end_timestamp
                and order_paid_at_utc >= DATE_SUB(week_start_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < week_end_timestamp
                and is_membership_order) as cancelled,

    countif(memberships.user_id is not null and (subscription_created_at_utc >= week_start_timestamp and subscription_created_at_utc <= week_end_timestamp)
            AND paid_order_rank > 1 and paid_membership_order_rank = 1
            and order_paid_at_utc >= week_start_timestamp and order_paid_at_utc <= week_end_timestamp
            ) as subs_alc_2_sub,

    ------------------ Active ALC Customers 90 --------------- 

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < week_start_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc ) or subscription_created_at_utc > week_start_timestamp  )
               and order_paid_at_utc >= TIMESTAMP_SUB(week_start_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < week_start_timestamp
               and is_ala_carte_order
            ) as week_start_active_alc_90_days,

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < week_end_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc )  or subscription_created_at_utc > week_end_timestamp )
               and order_paid_at_utc >= TIMESTAMP_SUB(week_end_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < week_end_timestamp
               and is_ala_carte_order
                ) as week_end_active_alc_90_days,
    
    countif(  o.is_ala_carte_order
                and o.paid_order_rank = 1
                and order_paid_at_utc >= week_start_timestamp and order_paid_at_utc <= week_end_timestamp
                and is_ala_carte_order
                ) as new_alc_customers,
    
    countif( --(subscription_created_at_utc > week_start_timestamp and subscription_created_at_utc < week_end_timestamp) and
             paid_order_rank > 1 and paid_membership_order_rank = 1
             and (order_paid_at_utc > week_start_timestamp and order_paid_at_utc < week_end_timestamp)
                ) as alc_alc_2_sub,

    ------------------ Active ALC Customers 180 --------------- 

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < week_start_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc ) or subscription_created_at_utc > week_start_timestamp  )
               and order_paid_at_utc >= TIMESTAMP_SUB(week_start_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < week_start_timestamp
               and is_ala_carte_order
            ) as week_start_active_alc_180_days,

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < week_end_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc )  or subscription_created_at_utc > week_end_timestamp )
               and order_paid_at_utc >= TIMESTAMP_SUB(week_end_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < week_end_timestamp
               and is_ala_carte_order
                ) as week_end_active_alc_180_days


    FROM
    weekly_calendar c
    INNER JOIN
    filtered_orders o ON timestamp(date(o.ORDER_PAID_AT_UTC , 'America/Los_Angeles')) <= c.week_end_timestamp
    LEFT JOIN memberships on o.user_id = memberships.user_id
    group by 1, 2, 3, 4, 5
)
,daily_final as (
    SELECT
    dc.fiscal_year,
    dc.fiscal_week_num,
    dc.calendar_date,
    day_start_timestamp as period_start,
    day_end_timestamp as period_end,
    o.user_id,
    ------------------------ Active subs 180 days -----------------
    countif(memberships.user_id is not null and subscription_created_at_utc < day_start_timestamp
               and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > day_start_timestamp)
               and order_paid_at_utc >= TIMESTAMP_SUB(day_start_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < day_start_timestamp
               and is_membership_order ) as day_start_active_subscribers_180_daily,
    countif(memberships.user_id is not null and subscription_created_at_utc < day_start_timestamp
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > day_start_timestamp)
                and order_paid_at_utc >= TIMESTAMP_SUB(day_start_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < day_start_timestamp
                and is_membership_order ) as day_end_active_subscribers_180_daily,
    ------------------------ Active subs 90 days -----------------
    countif(memberships.user_id is not null and subscription_created_at_utc < day_start_timestamp
               and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > day_start_timestamp)
               and order_paid_at_utc >= TIMESTAMP_SUB(day_start_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < day_start_timestamp
               and is_membership_order ) as day_start_active_subscribers_90_daily,

    countif(memberships.user_id is not null and subscription_created_at_utc < day_end_timestamp
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > day_end_timestamp)
                and order_paid_at_utc >= TIMESTAMP_SUB(day_end_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < day_end_timestamp
                and is_membership_order ) as day_end_active_subscribers_90_daily,

    countif(memberships.user_id is not null and (subscription_created_at_utc >= day_start_timestamp and subscription_created_at_utc <= day_end_timestamp)
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > day_start_timestamp)
                AND o.paid_order_rank = 1 AND o.paid_unique_membership_order_rank = 1
                and order_paid_at_utc >= day_start_timestamp and order_paid_at_utc <= day_end_timestamp
                and is_membership_order) as new_first_subscriber_daily,

    countif(memberships.user_id is not null and (subscription_created_at_utc >= day_start_timestamp and subscription_created_at_utc <= day_end_timestamp)
                and (subscription_cancelled_at_utc is null or subscription_cancelled_at_utc > day_start_timestamp)
                AND o.paid_order_rank > 1 AND o.paid_unique_membership_order_rank = 1
                and order_paid_at_utc >= day_start_timestamp and order_paid_at_utc <= day_end_timestamp
                and is_membership_order) as new_subscriber_daily,

    countif(memberships.user_id is not null and subscription_created_at_utc < day_start_timestamp
                and subscription_cancelled_at_utc >= day_start_timestamp and subscription_cancelled_at_utc <= day_end_timestamp
                and order_paid_at_utc >= DATE_SUB(day_start_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < day_end_timestamp
                and is_membership_order) as cancelled_daily,

    countif(memberships.user_id is not null and (subscription_created_at_utc >= day_start_timestamp and subscription_created_at_utc <= day_end_timestamp)
            AND paid_order_rank > 1 and paid_membership_order_rank = 1
            and order_paid_at_utc >= day_start_timestamp and order_paid_at_utc <= day_end_timestamp
            ) as subs_alc_2_sub_daily,

    ------------------ Active ALC Customers 90 --------------- 

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < day_start_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc ) or subscription_created_at_utc > day_start_timestamp  )
               and order_paid_at_utc >= TIMESTAMP_SUB(day_start_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < day_start_timestamp
               and is_ala_carte_order
            ) as day_start_active_alc_90_days_daily,

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < day_end_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc )  or subscription_created_at_utc > day_end_timestamp )
               and order_paid_at_utc >= TIMESTAMP_SUB(day_end_timestamp, INTERVAL 90 DAY) and order_paid_at_utc < day_end_timestamp
               and is_ala_carte_order
                ) as day_end_active_alc_90_days_daily,
    
    countif(  o.is_ala_carte_order
                and o.paid_order_rank = 1
                and order_paid_at_utc >= day_start_timestamp and order_paid_at_utc <= day_end_timestamp
                and is_ala_carte_order
                ) as new_alc_customers_daily,
    
    countif( --(subscription_created_at_utc > day_start_timestamp and subscription_created_at_utc < day_end_timestamp) and
             paid_order_rank > 1 and paid_membership_order_rank = 1
             and (order_paid_at_utc > day_start_timestamp and order_paid_at_utc < day_end_timestamp)
                ) as alc_alc_2_sub_daily,

    ------------------ Active ALC Customers 180 --------------- 

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < day_start_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc ) or subscription_created_at_utc > day_start_timestamp  )
               and order_paid_at_utc >= TIMESTAMP_SUB(day_start_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < day_start_timestamp
               and is_ala_carte_order
            ) as day_start_active_alc_180_days_daily,

    countif( ( memberships.user_id is null or (subscription_cancelled_at_utc < day_end_timestamp and subscription_created_at_utc < subscription_cancelled_at_utc )  or subscription_created_at_utc > day_end_timestamp )
               and order_paid_at_utc >= TIMESTAMP_SUB(day_end_timestamp, INTERVAL 180 DAY) and order_paid_at_utc < day_end_timestamp
               and is_ala_carte_order
                ) as day_end_active_alc_180_days_daily


    FROM
    daily_calendar dc
    INNER JOIN
    filtered_orders o 
      ON TIMESTAMP(DATE(o.ORDER_PAID_AT_UTC, 'America/Los_Angeles'), 'America/Los_Angeles') 
         <= dc.day_end_timestamp
    LEFT JOIN memberships on o.user_id = memberships.user_id
    group by 1, 2, 3, 4, 5, 6
)

-- Calculate revenue per bucket
,revenue_calculations AS (
    SELECT
        c.fiscal_year,
        c.fiscal_week_num,
        o.user_id,
        --o.order_id,
        sum(o.net_revenue) as net_revenue,
        count(distinct order_id) as total_orders,
        SUM(if(is_membership_order, o.net_revenue,null)) AS subs_net_revenue,
        SUM(if(is_ala_carte_order, o.net_revenue,null)) AS alc_net_revenue,
        COUNT(DISTINCT if(is_membership_order, o.order_id,null)) AS subs_total_orders,
        COUNT(DISTINCT if(is_ala_carte_order, o.order_id,null)) AS alc_total_orders
    FROM
        filtered_orders o
    left join 
        fiscal_calendar c ON (DATE(o.ORDER_PAID_AT_UTC , 'America/Los_Angeles')) = date(timestamp(calendar_date))
    group by 1,2,3

)
,daily_revenue_calculations AS (
    SELECT
        dc.fiscal_year,
        dc.fiscal_week_num,
        dc.calendar_date,
        o.user_id,
        sum(o.net_revenue) as net_revenue_daily,
        COUNT(DISTINCT o.order_id) as total_orders_daily,
        SUM(if(is_membership_order, o.net_revenue,null)) AS subs_net_revenue_daily,
        SUM(if(is_ala_carte_order, o.net_revenue,null)) AS alc_net_revenue_daily,
        COUNT(DISTINCT if(is_membership_order, o.order_id,null)) AS subs_total_orders_daily,
        COUNT(DISTINCT if(is_ala_carte_order, o.order_id,null)) AS alc_total_orders_daily
    FROM filtered_orders o
    LEFT JOIN daily_calendar dc
      ON DATE(o.order_paid_at_utc, 'America/Los_Angeles') = date(timestamp(calendar_date))
    GROUP BY 1, 2, 3, 4 
)

 , weekly_results as (
    select 
        weekly_final.fiscal_year,
        weekly_final.fiscal_week_num,
        'WEEKLY' AS period_type,
        weekly_final.period_start,
        weekly_final.period_end,
        count(distinct if(week_start_active_subscribers_90 > 0, weekly_final.user_id, null)) as week_start_active_subscribers_90,
        count(distinct if(week_end_active_subscribers_90 > 0, weekly_final.user_id, null)) as week_end_active_subscribers_90,
        count(distinct if(new_first_subscriber > 0, weekly_final.user_id, null)) as new_first_subscriber,
        count(distinct if(new_subscriber > 0 and subs_alc_2_sub = 0, weekly_final.user_id, null)) new_subscriber,
        count(distinct if(subs_alc_2_sub > 0, weekly_final.user_id, null)) as subs_alc_2_sub,
        count(distinct if(week_start_active_subscribers_90 = 0 and week_end_active_subscribers_90 > 0 and new_subscriber = 0 and cancelled = 0 and new_first_subscriber = 0, weekly_final.user_id, null)) as subs_reactivated,
        count(distinct if(cancelled > 0, weekly_final.user_id, null)) * -1  as total_memberships_cancelled,
        count(distinct if(week_start_active_subscribers_90 > 0 and week_end_active_subscribers_90 = 0 and cancelled = 0, weekly_final.user_id, null)) * -1  as subs_churned_90_days,

        sum(subs_total_orders) as total_orders_active_subscribers_90,
        sum(subs_net_revenue) as total_revenue_active_subscribers_90,

        ------ Subs 180 Days --- 
        count(distinct if(week_start_active_subscribers_180 > 0, weekly_final.user_id, null)) as week_start_active_subscribers_180,
        count(distinct if(week_end_active_subscribers_180 > 0, weekly_final.user_id, null)) as week_end_active_subscribers_180,
        count(distinct if(week_start_active_subscribers_180 = 0 and week_end_active_subscribers_180 > 0 and new_subscriber = 0 and cancelled = 0 and new_first_subscriber = 0, weekly_final.user_id, null)) as subs_reactivated_180,
        count(distinct if(week_start_active_subscribers_180 > 0 and week_end_active_subscribers_180 = 0 and cancelled = 0, weekly_final.user_id, null)) * -1  as subs_churned_180_days,



        ----- ALC 90 Days ---- 
        count(distinct if(week_start_active_alc_90_days > 0, weekly_final.user_id, null)) as week_start_active_alc_90_days,
        count(distinct if(week_end_active_alc_90_days > 0, weekly_final.user_id, null)) as week_end_active_alc_90_days,
        count(distinct if(new_alc_customers > 0 and alc_alc_2_sub = 0, weekly_final.user_id, null)) as new_alc_customers,
        count(distinct if(week_start_active_alc_90_days = 0 and week_end_active_alc_90_days > 0 and new_alc_customers = 0 and alc_alc_2_sub = 0 , weekly_final.user_id, null))  as alc_reactivated,
        (count(distinct if(week_start_active_alc_90_days > 0 and week_end_active_alc_90_days = 0 and alc_alc_2_sub = 0, weekly_final.user_id, null)) - count(distinct if(alc_alc_2_sub > 0, weekly_final.user_id, null))) * -1  as alc_churned_90_days,
        count(distinct if(alc_alc_2_sub > 0, weekly_final.user_id, null)) * -1 as alc_alc_2_sub,

        sum(alc_total_orders) as total_orders_active_alc_90_days,
        sum(alc_net_revenue) as total_revenue_active_alc_90_days,
    
        ----- ALC 180 Days ---- 
        count(distinct if(week_start_active_alc_180_days > 0, weekly_final.user_id, null)) as week_start_active_alc_180_days,
        count(distinct if(week_end_active_alc_180_days > 0, weekly_final.user_id, null)) as week_end_active_alc_180_days,
        --count(distinct if(new_alc_customers > 0 and alc_alc_2_sub = 0, weekly_final.user_id, null)) as new_alc_customers,
        count(distinct if(week_start_active_alc_180_days = 0 and week_end_active_alc_180_days > 0 and new_alc_customers = 0 and alc_alc_2_sub = 0 , weekly_final.user_id, null))  as alc_reactivated_180,
        (count(distinct if(week_start_active_alc_180_days > 0 and week_end_active_alc_180_days = 0 and alc_alc_2_sub = 0, weekly_final.user_id, null)) - count(distinct if(alc_alc_2_sub > 0, weekly_final.user_id, null))) * -1  as alc_churned_180_days,
        --count(distinct if(alc_alc_2_sub > 0, weekly_final.user_id, null)) * -1 as alc_alc_2_sub,

        sum(alc_total_orders) as total_orders_active_alc_180_days,
        sum(alc_net_revenue) as total_revenue_active_alc_180_days


    from weekly_final 
    left join revenue_calculations on 
    weekly_final.fiscal_year = revenue_calculations.fiscal_year 
    and weekly_final.fiscal_week_num = revenue_calculations.fiscal_week_num 
    and weekly_final.user_id = revenue_calculations.user_id
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY period_start DESC

)
 ,daily_results AS (
    SELECT
        df.fiscal_year,
        df.fiscal_week_num,
        'DAILY' AS period_type,
        df.period_start,
        df.period_end,
        count(distinct if(day_start_active_subscribers_90_daily > 0, df.user_id, null)) AS day_start_active_subscribers_90_daily,
        count(distinct if(day_end_active_subscribers_90_daily > 0, df.user_id, null)) AS day_end_active_subscribers_90_daily,
        count(distinct if(new_first_subscriber_daily > 0, df.user_id, null)) AS new_first_subscriber_daily,
        count(distinct if(new_subscriber_daily > 0 and subs_alc_2_sub_daily = 0, df.user_id, null)) AS new_subscriber_daily,
        count(distinct if(subs_alc_2_sub_daily > 0, df.user_id, null)) AS subs_alc_2_sub_daily,
        count(distinct if(day_start_active_subscribers_90_daily = 0 and day_end_active_subscribers_90_daily > 0 and new_subscriber_daily = 0 and cancelled_daily = 0 and new_first_subscriber_daily = 0, df.user_id, null)) AS subs_reactivated_daily,
        count(distinct if(cancelled_daily > 0, df.user_id, null)) * -1 AS total_memberships_cancelled_daily,
        count(distinct if(day_start_active_subscribers_90_daily > 0 and day_end_active_subscribers_90_daily = 0 and cancelled_daily = 0, df.user_id, null)) * -1 AS subs_churned_90_days_daily,

        sum(subs_total_orders_daily) AS total_orders_active_subscribers_90_daily,
        sum(subs_net_revenue_daily) AS total_revenue_active_subscribers_90_daily,

        ---- Subs 180 Days --- 
    
        count(distinct if(day_start_active_subscribers_180_daily > 0, df.user_id, null)) as day_start_active_subscribers_180_daily,
        count(distinct if(day_end_active_subscribers_180_daily > 0, df.user_id, null)) AS day_end_active_subscribers_180_daily,
        count(distinct if(day_start_active_subscribers_180_daily = 0 and day_end_active_subscribers_180_daily > 0 and new_subscriber_daily = 0 and cancelled_daily = 0 and new_first_subscriber_daily = 0, df.user_id, null)) AS subs_reactivated_daily_180,
        count(distinct if(day_start_active_subscribers_180_daily > 0 and day_end_active_subscribers_180_daily = 0 and cancelled_daily = 0, df.user_id, null)) * -1 AS subs_churned_180_days_daily,


        ----- ALC 90 Days ---- 
        count(distinct IF(day_start_active_alc_90_days_daily > 0, df.user_id, NULL)) AS day_start_active_alc_90_days_daily,
        count(distinct IF(day_end_active_alc_90_days_daily > 0, df.user_id, NULL)) AS day_end_active_alc_90_days_daily,
        count(distinct IF(new_alc_customers_daily > 0 and alc_alc_2_sub_daily = 0, df.user_id, NULL)) AS new_alc_customers_daily,
        count(distinct IF(day_start_active_alc_90_days_daily = 0 and day_end_active_alc_90_days_daily > 0 and new_alc_customers_daily = 0 and alc_alc_2_sub_daily = 0, df.user_id, NULL)) AS alc_reactivated_daily,
        (count(distinct IF(day_start_active_alc_90_days_daily > 0 and day_end_active_alc_90_days_daily = 0 and alc_alc_2_sub_daily = 0, df.user_id, NULL)) - count(distinct IF(alc_alc_2_sub_daily > 0, df.user_id, NULL))) * -1 AS alc_churned_90_days_daily,
        count(distinct IF(alc_alc_2_sub_daily > 0, df.user_id, NULL)) * -1 AS alc_alc_2_sub_daily,

        sum(alc_total_orders_daily) AS total_orders_active_alc_90_days_daily,
        sum(alc_net_revenue_daily) AS total_revenue_active_alc_90_days_daily,

        ----- ALC 180 Days ---- 
        count(distinct IF(day_start_active_alc_180_days_daily > 0, df.user_id, NULL)) AS day_start_active_alc_180_days_daily,
        count(distinct IF(day_end_active_alc_180_days_daily > 0, df.user_id, NULL)) AS day_end_active_alc_180_days_daily,
        count(distinct IF(day_start_active_alc_180_days_daily = 0 and day_end_active_alc_180_days_daily > 0 and new_alc_customers_daily = 0 and alc_alc_2_sub_daily = 0, df.user_id, NULL)) AS alc_reactivated_180_daily,
        (count(distinct IF(day_start_active_alc_180_days_daily > 0 and day_end_active_alc_180_days_daily = 0 and alc_alc_2_sub_daily = 0, df.user_id, NULL)) - count(distinct IF(alc_alc_2_sub_daily > 0, df.user_id, NULL))) * -1 AS alc_churned_180_days_daily,
         
        sum(alc_total_orders_daily) AS total_orders_active_alc_180_days_daily,
        sum(alc_net_revenue_daily) AS total_revenue_active_alc_180_days_daily

    FROM daily_final df 
    LEFT JOIN daily_revenue_calculations dr ON df.fiscal_year = dr.fiscal_year
      AND df.fiscal_week_num = dr.fiscal_week_num
      AND df.calendar_date = dr.calendar_date
      AND df.user_id = dr.user_id 
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY period_start DESC
)


-- Combine weekly and daily metrics
SELECT * FROM weekly_results
UNION ALL
SELECT * FROM daily_results
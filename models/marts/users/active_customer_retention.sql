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
,calendar AS (
          SELECT
              fiscal_year,
              fiscal_week_num,
              timestamp(calendar_date) as calendar_date,
              timestamp(date(fc.calendar_date_week_sun)) AS week_start_timestamp,
              timestamp(DATE_ADD(fc.calendar_date_week_sun, INTERVAL 7 DAY)) AS week_end_timestamp,
          FROM fiscal_calendar fc
          WHERE calendar_date <= CURRENT_DATE()
)
,final as (
    SELECT
    c.fiscal_year,
    c.fiscal_week_num,
    o.user_id,
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
    ------------------ Active ALC Customers 180 --------------- 

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
                ) as alc_alc_2_sub


    FROM
    calendar c
    INNER JOIN
    filtered_orders o ON timestamp(date(o.ORDER_PAID_AT_UTC , 'America/Los_Angeles')) <= c.week_end_timestamp
    LEFT JOIN memberships on o.user_id = memberships.user_id
    group by 1, 2, 3
)

-- Calculate revenue per bucket
,revenue_calculations AS (
    SELECT
        c.fiscal_year,
        c.fiscal_week_num,
        o.user_id,
        --o.order_id,
        sum(o.net_revenue) as net_revenue,
        count(distinct order_id) as total_orders
    FROM
        filtered_orders o
    left join 
        fiscal_calendar c ON (DATE(o.ORDER_PAID_AT_UTC , 'America/Los_Angeles')) = date(calendar_date)
    group by 1,2,3

)

, results as (
    select 
        final.fiscal_year,
        final.fiscal_week_num,
        count(distinct if(week_start_active_subscribers_90 > 0, final.user_id, null)) as week_start_active_subscribers_90,
        count(distinct if(week_end_active_subscribers_90 > 0, final.user_id, null)) as week_end_active_subscribers_90,
        count(distinct if(new_first_subscriber > 0, final.user_id, null)) as new_first_subscriber,
        count(distinct if(new_subscriber > 0 and subs_alc_2_sub = 0, final.user_id, null)) new_subscriber,
        count(distinct if(subs_alc_2_sub > 0, final.user_id, null)) as subs_alc_2_sub,
        count(distinct if(week_start_active_subscribers_90 = 0 and week_end_active_subscribers_90 > 0 and new_subscriber = 0 and cancelled = 0 and new_first_subscriber = 0, final.user_id, null)) as subs_reactivated,
        count(distinct if(cancelled > 0, final.user_id, null)) * -1  as total_memberships_cancelled,
        count(distinct if(week_start_active_subscribers_90 > 0 and week_end_active_subscribers_90 = 0 and cancelled = 0, final.user_id, null)) * -1  as subs_churned_90_days,

        sum(if(week_end_active_subscribers_90 > 0, revenue_calculations.total_orders, null)) as total_orders_active_subscribers_90,
        sum(if(week_end_active_subscribers_90 > 0, revenue_calculations.net_revenue, null)) as total_revenue_active_subscribers_90,


        ----- ALC ---- 
        count(distinct if(week_start_active_alc_90_days > 0, final.user_id, null)) as week_start_active_alc_90_days,
        count(distinct if(week_end_active_alc_90_days > 0, final.user_id, null)) as week_end_active_alc_90_days,
        count(distinct if(new_alc_customers > 0 and alc_alc_2_sub = 0, final.user_id, null)) as new_alc_customers,
        count(distinct if(week_start_active_alc_90_days = 0 and week_end_active_alc_90_days > 0 and new_alc_customers = 0 and alc_alc_2_sub = 0 , final.user_id, null))  as alc_reactivated,
        (count(distinct if(week_start_active_alc_90_days > 0 and week_end_active_alc_90_days = 0 and alc_alc_2_sub = 0, final.user_id, null)) - count(distinct if(alc_alc_2_sub > 0, final.user_id, null))) * -1  as alc_churned_90_days,
        count(distinct if(alc_alc_2_sub > 0, final.user_id, null)) * -1 as alc_alc_2_sub,

        sum(if(week_end_active_alc_90_days > 0, revenue_calculations.total_orders, null)) as total_orders_active_alc_90_days,
        sum(if(week_end_active_alc_90_days > 0, revenue_calculations.net_revenue, null)) as total_revenue_active_alc_90_days

    from final 
    left join revenue_calculations on 
    final.fiscal_year = revenue_calculations.fiscal_year 
    and final.fiscal_week_num = revenue_calculations.fiscal_week_num 
    and final.user_id = revenue_calculations.user_id
    group by 1, 2
    order by 1 desc, 2 desc

)


select
*
/*
    fiscal_year,
    fiscal_week_num,
    
    week_start_active_subscribers_90,
    week_end_active_subscribers_90,
    new_first_subscriber,
    new_subscriber,
    subs_alc_2_sub,
    subs_reactivated,
    total_memberships_cancelled,
    subs_churned_90_days
    
    week_start_active_alc_180_days,
    week_end_active_alc_180_days,
    new_alc_customers,
    alc_reactivated,
    alc_churned_180_days,
    alc_alc_2_sub
*/

from results

/*
-- Final aggregation combining user counts and revenues
SELECT
    af.fiscal_year,
    af.fiscal_week_num,
    af.total_active_users_last_180_days,
    rc.total_active_users_revenue_last_180_days,
    af.total_active_members_last_180_days,
    
    af.active_subscribers_180_days,
    rc.active_subscribers_180_days_revenue,
    rc.active_subscribers_orders,
    af.active_cancelled_subscribers_180_days,

    af.active_alc_180_days,
    rc.active_alc_180_days_revenue,
    rc.active_alc_orders
FROM
    aggregated_flags af
LEFT JOIN (
    SELECT
        fiscal_year,
        fiscal_week_num,
        SUM(total_active_users_revenue) AS total_active_users_revenue_last_180_days,
        SUM(active_subscribers_revenue) AS active_subscribers_180_days_revenue,
        SUM(active_alc_revenue) AS active_alc_180_days_revenue,
        COUNT(DISTINCT active_subscribers_orders) as active_subscribers_orders,
        COUNT(DISTINCT active_alc_orders) as active_alc_orders
    FROM
        revenue_calculations
    GROUP BY
        fiscal_year,
        fiscal_week_num
) rc
    ON af.fiscal_year = rc.fiscal_year
    AND af.fiscal_week_num = rc.fiscal_week_num
ORDER BY
    af.fiscal_year,
    af.fiscal_week_num DESC  */
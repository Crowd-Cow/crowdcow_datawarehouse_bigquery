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

,users_subset as (select user_id from {{ ref('users') }})
,memberships_subset as (
        select   
        subscription_id,
        user_id,
        subscription_created_at_utc,
        subscription_cancelled_at_utc 
        from {{ ref('memberships') }})

,calendar AS (
          SELECT
              fiscal_year,
              fiscal_week_num,
              timestamp(calendar_date) as calendar_date,
              timestamp(date(fc.calendar_date_week_sun)) AS week_end_timestamp,
              timestamp(date(fc.calendar_date_week)) AS week_start_timestamp,
              timestamp(DATE_SUB(calendar_date, INTERVAL 180 DAY)) AS date_180_days_ago,
              timestamp(DATE_SUB(calendar_date, INTERVAL 90 DAY)) AS date_90_days_ago
          FROM fiscal_calendar fc
          WHERE calendar_date <= CURRENT_DATE()
)

-- Pre-aggregate user memberships to minimize joins later
,user_memberships AS (
    SELECT
        u.user_id,
        m.subscription_created_at_utc,
        m.subscription_cancelled_at_utc
    FROM users_subset u
    LEFT JOIN memberships_subset m
        ON u.user_id = m.user_id
),
-- Precompute active user flags per fiscal week
active_user_flags AS (
    SELECT distinct
        c.fiscal_year,
        c.fiscal_week_num,
        o.user_id,
        -- Total Active Users
        1 AS is_total_active_user,

        -- Total Active Members
        CASE
            WHEN m.subscription_created_at_utc <= c.week_end_timestamp
                 AND o.order_paid_at_utc >= c.date_180_days_ago
                 AND o.subscription_id IS NOT NULL
            THEN 1
            ELSE 0
        END AS is_total_active_member,

        -- Active Subscribers
        CASE
            WHEN m.subscription_created_at_utc <= c.week_end_timestamp
                 AND o.order_paid_at_utc >= c.date_180_days_ago
                 AND o.subscription_id IS NOT NULL
                 AND (m.subscription_cancelled_at_utc IS NULL
                      OR m.subscription_cancelled_at_utc >= c.week_end_timestamp)
            THEN 1
            ELSE 0
        END AS is_active_subscriber,

        -- Active Cancelled Subscribers
        CASE
            WHEN m.subscription_created_at_utc <= c.week_end_timestamp
                 AND o.order_paid_at_utc >= c.date_180_days_ago
                 AND o.subscription_id IS NOT NULL
                 AND m.subscription_cancelled_at_utc BETWEEN c.date_180_days_ago AND c.week_end_timestamp
            THEN 1
            ELSE 0
        END AS is_active_cancelled_subscriber,

        -- Active ALC
        CASE
            WHEN o.order_paid_at_utc >= c.date_180_days_ago
                 AND o.subscription_id IS NULL
                 AND (m.subscription_created_at_utc IS NULL
                      OR (m.subscription_cancelled_at_utc IS NOT NULL
                          AND m.subscription_cancelled_at_utc <= c.date_180_days_ago))
            THEN 1
            ELSE 0
        END AS is_active_alc
    FROM
        calendar c
    INNER JOIN
        filtered_orders o
        ON timestamp(date(o.ORDER_PAID_AT_UTC , 'America/Los_Angeles')) <= c.week_end_timestamp
    LEFT JOIN
        user_memberships m
        ON o.user_id = m.user_id
),
-- Aggregate flags per week and bucket
aggregated_flags AS (
    SELECT
        af.fiscal_year,
        af.fiscal_week_num,
        COUNT(DISTINCT IF(af.is_total_active_user = 1, af.user_id, NULL)) AS total_active_users_last_180_days,
        COUNT(DISTINCT IF(af.is_total_active_member = 1, af.user_id, NULL)) AS total_active_members_last_180_days,
        COUNT(DISTINCT IF(af.is_active_subscriber = 1, af.user_id, NULL)) AS active_subscribers_180_days,
        COUNT(DISTINCT IF(af.is_active_cancelled_subscriber = 1, af.user_id, NULL)) AS active_cancelled_subscribers_180_days,
        COUNT(DISTINCT IF(af.is_active_alc = 1, af.user_id, NULL)) AS active_alc_180_days
    FROM
        active_user_flags af
    GROUP BY
        af.fiscal_year,
        af.fiscal_week_num
),
-- Calculate revenue per bucket
revenue_calculations AS (
    SELECT
        c.fiscal_year,
        c.fiscal_week_num,
        o.order_id,
        o.net_revenue AS total_active_users_revenue,
        CASE
            WHEN is_membership_order THEN o.net_revenue
            ELSE null
        END AS active_subscribers_revenue,
        CASE
            WHEN is_ala_carte_order THEN o.net_revenue
            ELSE null
        END AS active_alc_revenue,
        CASE
            WHEN is_membership_order THEN o.order_id
            ELSE null
        END AS active_subscribers_orders,
        CASE
            WHEN is_ala_carte_order THEN o.order_id
            ELSE null
        END AS active_alc_orders
    FROM
        filtered_orders o
    left join 
        fiscal_calendar c ON (DATE(o.ORDER_PAID_AT_UTC , 'America/Los_Angeles')) = date(calendar_date)

)
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
    af.fiscal_week_num DESC
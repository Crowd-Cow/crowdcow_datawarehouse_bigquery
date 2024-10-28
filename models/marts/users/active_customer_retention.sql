with fiscal_calendar as (select * from {{ ref('retail_calendar') }} where fiscal_year > 2022) 
,orders as (select * from {{ ref('orders') }} 
        where order_type = 'E-COMMERCE'
         AND (NOT is_rastellis OR is_rastellis IS NULL)
         AND (NOT is_qvc OR is_qvc IS NULL)
         AND (NOT is_seabear OR is_seabear IS NULL)
         AND (NOT is_backyard_butchers OR is_backyard_butchers IS NULL)
         AND is_paid_order
         AND not is_cancelled_order
         AND cast(order_paid_at_utc as date) >= '2022-01-01'  )
,users as (select * from {{ ref('users') }})
,memberships as (select * from {{ ref('memberships') }})

,calendar AS (
          SELECT
              fiscal_year,
              fiscal_week_num,
              timestamp(calendar_date) as calendar_date,
              timestamp(DATE_SUB(calendar_date, INTERVAL 180 DAY)) AS date_180_days_ago,
              timestamp(DATE_SUB(calendar_date, INTERVAL 90 DAY)) AS date_90_days_ago
          FROM fiscal_calendar
          WHERE calendar_date <= CURRENT_DATE()
)

SELECT
    rc.fiscal_year as fiscal_year,
    rc.fiscal_week_num as fiscal_week_num,
    COUNT(DISTINCT CASE
        WHEN o.order_paid_at_utc >= rc.date_180_days_ago THEN o.user_id
    END) AS total_active_users_last_180_days,

    COUNT(DISTINCT CASE
        WHEN memberships.subscription_created_at_utc <= rc.calendar_date
            AND o.order_paid_at_utc >= rc.date_180_days_ago
            AND o.subscription_id IS NOT NULL THEN o.user_id
    END) AS total_active_members_last_180_days,

    COUNT(DISTINCT CASE
        WHEN memberships.subscription_created_at_utc <= rc.calendar_date
            AND o.order_paid_at_utc >= rc.date_180_days_ago
            AND o.subscription_id is not null
            AND (memberships.subscription_cancelled_at_utc is null or memberships.subscription_cancelled_at_utc >= rc.calendar_date )
            THEN o.user_id
    END) AS active_subscribers_180_days,

    COUNT(DISTINCT CASE
        WHEN memberships.subscription_created_at_utc <= rc.calendar_date
            AND o.order_paid_at_utc >= rc.date_180_days_ago
            AND (memberships.subscription_cancelled_at_utc <= rc.calendar_date and memberships.subscription_cancelled_at_utc >= rc.date_180_days_ago )
            AND o.subscription_id IS NOT NULL
            THEN o.user_id
    END) AS active_cancelled_subscribers_180_days,

    COUNT(DISTINCT CASE
        WHEN o.order_paid_at_utc >= rc.date_180_days_ago
            AND o.subscription_id is null 
            AND ( (user_memberships.subscription_created_at_utc IS NULL) OR (user_memberships.subscription_cancelled_at_utc IS NOT NULL AND user_memberships.subscription_cancelled_at_utc <= rc.date_180_days_ago))
            THEN o.user_id
    END) AS active_alc_180_days,
--------------- 90 Days -----------
    COUNT(DISTINCT CASE
        WHEN o.order_paid_at_utc >= rc.date_90_days_ago THEN o.user_id
    END) AS total_active_users_last_90_days,

    COUNT(DISTINCT CASE
        WHEN memberships.subscription_created_at_utc <= rc.calendar_date
            AND o.order_paid_at_utc >= rc.date_90_days_ago
            AND o.subscription_id IS NOT NULL THEN o.user_id
    END) AS total_active_members_last_90_days,

    COUNT(DISTINCT CASE
        WHEN memberships.subscription_created_at_utc <= rc.calendar_date
            AND o.order_paid_at_utc >= rc.date_90_days_ago
            AND o.subscription_id is not null
            AND (memberships.subscription_cancelled_at_utc is null or memberships.subscription_cancelled_at_utc >= rc.calendar_date )
            THEN o.user_id
    END) AS active_subscribers_90_days,

    COUNT(DISTINCT CASE
        WHEN memberships.subscription_created_at_utc <= rc.calendar_date
            AND o.order_paid_at_utc >= rc.date_90_days_ago
            AND (memberships.subscription_cancelled_at_utc <= rc.calendar_date and memberships.subscription_cancelled_at_utc >= rc.date_90_days_ago )
            AND o.subscription_id IS NOT NULL
            THEN o.user_id
    END) AS active_cancelled_subscribers_90_days,

    COUNT(DISTINCT CASE
        WHEN o.order_paid_at_utc >= rc.date_90_days_ago
            AND o.subscription_id is null 
            AND ( (user_memberships.subscription_created_at_utc IS NULL) OR (user_memberships.subscription_cancelled_at_utc IS NOT NULL AND user_memberships.subscription_cancelled_at_utc <= rc.date_90_days_ago))
            THEN o.user_id
    END) AS active_alc_90_days

FROM
    calendar rc
INNER JOIN orders AS o
    ON o.order_paid_at_utc <= timestamp(rc.calendar_date)
LEFT JOIN users AS u
    ON o.user_id = u.user_id
LEFT JOIN memberships AS memberships
    ON o.subscription_id = memberships.subscription_id
LEFT JOIN memberships AS user_memberships
    ON u.user_id = user_memberships.user_id
GROUP BY
    rc.fiscal_year,
    rc.fiscal_week_num
ORDER BY 1,2 desc 
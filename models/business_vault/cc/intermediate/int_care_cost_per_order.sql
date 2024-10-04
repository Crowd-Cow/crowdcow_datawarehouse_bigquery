with 

care_costs as (select * from {{ ref ( 'stg_gs__fc_care_packaging_costs' ) }} where cost_type = 'CS_LABOR_COST')
,shipments as (select order_id, max(shipped_at_utc) as shipped_at_utc from {{ ref('stg_cc__shipments') }} where shipped_at_utc is not null group by 1)

, get_monthly_shipments AS (
    SELECT
        DATE_TRUNC(shipped_at_utc, MONTH) AS shipped_month,
        COUNT(DISTINCT order_id) AS order_count
    FROM shipments
    WHERE shipped_at_utc IS NOT NULL
    GROUP BY 1
),

get_monthly_care_costs AS (
    SELECT
        get_monthly_shipments.*,
        care_costs.cost_usd,
        SAFE_DIVIDE(care_costs.cost_usd, get_monthly_shipments.order_count) AS care_cost_per_order,
        IFNULL(LEAD(month_of_costs, 1) OVER (ORDER BY month_of_costs), '2999-01-01') AS adjusted_date
    FROM get_monthly_shipments
    INNER JOIN care_costs ON get_monthly_shipments.shipped_month = care_costs.month_of_costs
)

SELECT
    shipments.order_id,
    ROUND(get_monthly_care_costs.care_cost_per_order, 2) AS order_care_cost
FROM shipments
LEFT JOIN get_monthly_care_costs ON shipments.shipped_at_utc >= get_monthly_care_costs.shipped_month
    AND shipments.shipped_at_utc < get_monthly_care_costs.adjusted_date

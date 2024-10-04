with 

coolant_costs as (select * from {{ ref('stg_gs__fc_care_packaging_costs')}} where cost_type = 'COOLANT_COST')
,orders as (select * from {{ ref('stg_cc__orders') }})
,shipments as (select order_id, max(shipped_at_utc) as shipped_at_utc from {{ ref('stg_cc__shipments') }} where shipped_at_utc is not null group by 1)

,get_coolant_used AS (
    SELECT
        orders.order_id,
        orders.fc_id,
        orders.coolant_weight_in_pounds,
        shipments.shipped_at_utc,
        DATE_TRUNC(shipments.shipped_at_utc, MONTH) AS shipped_month
    FROM orders
    LEFT JOIN shipments ON orders.order_id = shipments.order_id
),
get_monthly_coolant_usage AS (
    SELECT
        shipped_month,
        fc_id,
        SUM(coolant_weight_in_pounds) AS total_monthly_coolant_pounds,
        COUNT(DISTINCT order_id) AS order_count
    FROM get_coolant_used
    GROUP BY 1, 2
),
calc_cost_per_pound AS (
    SELECT
        get_monthly_coolant_usage.*,
        coolant_costs.cost_usd,
        IFNULL(LEAD(coolant_costs.month_of_costs, 1) OVER (PARTITION BY coolant_costs.fc_id ORDER BY coolant_costs.month_of_costs), '2999-01-01') AS adjusted_date,
        SAFE_DIVIDE(cost_usd, total_monthly_coolant_pounds) AS coolant_cost_per_pound
    FROM get_monthly_coolant_usage
    INNER JOIN coolant_costs ON get_monthly_coolant_usage.shipped_month = coolant_costs.month_of_costs
        AND get_monthly_coolant_usage.fc_id = coolant_costs.fc_id
)

SELECT
    get_coolant_used.order_id,
    ROUND(get_coolant_used.coolant_weight_in_pounds * calc_cost_per_pound.coolant_cost_per_pound, 2) AS order_coolant_cost
FROM get_coolant_used
LEFT JOIN calc_cost_per_pound ON get_coolant_used.shipped_month >= calc_cost_per_pound.shipped_month
    AND get_coolant_used.shipped_month < calc_cost_per_pound.adjusted_date
    AND get_coolant_used.fc_id = calc_cost_per_pound.fc_id
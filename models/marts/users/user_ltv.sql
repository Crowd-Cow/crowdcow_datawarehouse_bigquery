with

purchasing_user as ( select * from {{ ref('users') }} where customer_cohort_date is not null )
,paid_orders as ( select * from {{ ref('orders') }} where is_paid_order and not is_cancelled_order)

,calc_margin as (
    select
        order_id
        ,is_rastellis
        ,user_id
        ,order_paid_at_utc
        ,net_product_revenue - product_cost as product_profit
        ,div0(product_profit,gross_product_revenue) as product_margin
        ,net_revenue - product_Cost - shipment_cost - packaging_cost - payment_processing_cost - coolant_cost
            - care_cost - fc_labor_cost - poseidon_fulfillment_cost as gross_profit
        ,div0(gross_profit,net_revenue) as gross_margin
    from paid_orders
)

select
    purchasing_user.user_id
    ,calc_margin.order_id
    ,calc_margin.is_rastellis
    ,datediff(month,customer_cohort_date,order_paid_at_utc) as customer_cohort_months
    ,datediff(day,customer_cohort_date,order_paid_at_utc) as customer_cohort_days
    ,datediff(month,customer_cohort_date,sysdate()) as customer_cohort_tenure_months
    ,datediff(month,membership_cohort_date,sysdate()) as membership_cohort_tenure_months
    ,iff(order_paid_at_utc >= membership_cohort_date,datediff(month,membership_cohort_date,order_paid_at_utc),null) as membership_cohort_months
    ,iff(order_paid_at_utc >= membership_cohort_date,datediff(day,membership_cohort_date,order_paid_at_utc),null) as membership_cohort_days
    ,calc_margin.product_profit
    ,calc_margin.product_margin
    ,calc_margin.gross_profit
    ,calc_margin.gross_margin
from purchasing_user
    left join calc_margin on purchasing_user.user_id = calc_margin.user_id

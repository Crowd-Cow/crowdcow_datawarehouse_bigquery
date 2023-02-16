with

purchasing_user as ( select * from {{ ref('users') }} where customer_cohort_date is not null )
,paid_orders as ( select * from {{ ref('orders') }} where is_paid_order and not is_cancelled_order)

,calc_margin as (
    select
        order_id
        ,is_rastellis
        ,is_qvc
        ,user_id
        ,order_paid_at_utc
        ,product_profit
        ,div0(product_profit,gross_product_revenue) as product_margin
        ,gross_profit
        ,div0(gross_profit,net_revenue) as gross_margin
        ,net_revenue
        
        ,round(
            gross_product_revenue 
            + membership_discount 
            + merch_discount
            + free_protein_promotion
            + item_promotion
            + shipping_revenue 
            + free_shipping_discount
            {# + new_member_discount #}
            + refund_amount
            + gift_redemption
            + other_discount
        ,2) as adjusted_net_revenue
        
        ,adjusted_net_revenue 
            - product_cost 
            - shipment_cost 
            - packaging_cost 
            - payment_processing_cost
            - coolant_cost 
            - care_cost 
            - fc_labor_cost 
            - poseidon_fulfillment_cost 
            - inbound_shipping_cost as adjusted_gross_profit
            
    from paid_orders
)

select
    purchasing_user.user_id
    ,calc_margin.order_id
    ,calc_margin.is_rastellis
    ,calc_margin.is_qvc
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
    ,calc_margin.net_revenue
    ,calc_margin.adjusted_net_revenue
    ,calc_margin.adjusted_gross_profit
from purchasing_user
    left join calc_margin on purchasing_user.user_id = calc_margin.user_id

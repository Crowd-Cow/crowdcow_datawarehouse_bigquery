{{
  config(
    snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

user as ( select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null )
,order_info as ( select * from {{ ref('orders') }} )
,order_item_units as ( select * from {{ ref('int_order_units_pct') }} )
,reward as ( select * from {{ ref('stg_cc__reward_points') }} )


,user_order_activity as (
    select
        user_id
        ,is_rastellis
        ,is_qvc
        ,count(order_id) as total_order_count
        ,count_if(is_completed_order and is_membership_order) as total_completed_membership_orders
        ,count_if(is_paid_order and not is_cancelled_order and is_ala_carte_order) as total_paid_ala_carte_order_count
        ,count_if(is_paid_order and not is_cancelled_order and is_membership_order) total_paid_membership_order_count
        ,count_if(is_paid_order and not is_cancelled_order and is_membership_order and sysdate()::date - order_paid_at_utc::date <= 90) as last_90_days_paid_membership_order_count
        ,sum(iff(is_paid_order and not is_cancelled_order,net_revenue,0)) as lifetime_net_revenue
        ,count_if(is_paid_order and not is_cancelled_order) as lifetime_paid_order_count
        ,count_if(completed_order_rank = 1 and not is_paid_order and not is_cancelled_order) as total_completed_unpaid_uncancelled_orders
        ,count_if(is_gift_order and is_paid_order and not is_cancelled_order) as total_paid_gift_order_count
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-6,sysdate()),net_revenue,0)) as six_month_net_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-6,sysdate()),gross_profit,0)) as six_month_gross_profit
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-12,sysdate()),net_revenue,0)) as twelve_month_net_revenue
        ,count_if(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-6,sysdate())) as six_month_paid_order_count
        ,count_if(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-12,sysdate())) as twelve_month_purchase_count
        ,count_if(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('day',-90,sysdate())) as last_90_days_paid_order_count
        ,count_if(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('day',-180,sysdate())) as last_180_days_paid_order_count
        ,count_if(has_been_delivered and delivered_at_utc >= dateadd('day',-7,sysdate())) as recent_delivered_order_count
        ,min(iff(is_paid_order and not is_cancelled_order,order_paid_at_utc::date,null)) as customer_cohort_date
        ,min(iff(is_paid_order and not is_cancelled_order and is_membership_order,order_paid_at_utc::date,null)) as membership_cohort_date
        ,max(iff(is_paid_order and not is_cancelled_order and is_membership_order,order_paid_at_utc::date,null)) as last_paid_membership_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and is_ala_carte_order,order_paid_at_utc::date,null)) as last_paid_ala_carte_order_date
        ,max(iff(is_paid_order and not is_cancelled_order,order_paid_at_utc::date,null)) as last_paid_order_date
        ,min(iff(completed_order_rank = 1,order_checkout_completed_at_utc,null)) as first_completed_order_date
        ,min(iff(completed_order_rank = 1,visit_id,null)) as first_completed_order_visit_id
        ,max(iff(is_paid_order and not is_cancelled_order,order_token,null)) as most_recent_order
        ,count_if(
            not is_gift_order
            and not is_gift_card_order
            and not is_bulk_gift_order
            and not is_cancelled_order
            and is_paid_order
            and (billing_state = 'CA' or order_delivery_state = 'CA')
         ) as total_california_orders
        ,avg(iff(is_paid_order and not is_cancelled_order,net_revenue,null)) as user_average_order_value
    from order_info
    group by 1,2,3
)

,user_percentiles as (
    select
        *
        ,ntile(100) over(partition by six_month_net_revenue > 0 order by six_month_net_revenue) as six_month_net_revenue_percentile
        ,ntile(100) over(partition by six_month_paid_order_count > 0 order by six_month_gross_profit) as six_month_gross_profit_percentile
        ,ntile(100) over(partition by twelve_month_net_revenue > 0 order by twelve_month_net_revenue) as twelve_month_net_revenue_percentile
        ,ntile(100) over(partition by lifetime_net_revenue > 0 order by lifetime_net_revenue) as lifetime_net_revenue_percentile
    from user_order_activity
)

,order_frequency as (
    select
        user_id
        
        ,lead(case when paid_order_rank is not null then order_paid_at_utc::date end,1) 
            over (partition by user_id order by paid_order_rank)  - order_paid_at_utc::date as days_to_next_paid_order
        
        ,lead(case when paid_membership_order_rank is not null then order_paid_at_utc::date end,1) 
            over (partition by user_id order by paid_membership_order_rank)  - order_paid_at_utc::date as days_to_next_paid_membership_order
        
        ,lead(case when paid_ala_carte_order_rank is not null then order_paid_at_utc::date end,1) 
            over (partition by user_id order by paid_ala_carte_order_rank)  - order_paid_at_utc::date as days_to_next_paid_ala_carte_order
    from order_info
)

,average_order_days as (
    select
        user_id
        ,avg(days_to_next_paid_order) as average_order_frequency_days
        ,avg(days_to_next_paid_membership_order) as average_membership_order_frequency_days
        ,avg(days_to_next_paid_ala_carte_order) as average_ala_carte_order_frequency_days
    from order_frequency
    group by 1
)

,user_order_item_activity as (
    select
        order_info.user_id
        
        ,sum(
            iff(
                order_info.is_paid_order 
                and not order_info.is_cancelled_order
                ,order_item_units.japanese_wagyu_revenue
                ,0
            )
        ) as lifetime_japanese_wagyu_revenue

    from order_item_units
        inner join order_info on order_item_units.order_id = order_info.order_id
    group by 1
)

,user_reward_activity as (
    select
        user_id
        ,iff(reward_reason = 'REDEMPTION',1,0) as has_redeemed_moolah 
        ,sum(iff(rewards_program = 'WAGYU_CLUB',reward_spend_amount,0)) as japanese_buyers_club_revenue
        ,(sum(iff(rewards_program = 'MOOLAH',reward_spend_amount,0))*100)::int as moolah_points
        ,(sum(iff(rewards_program = 'MOOLAH' and reward_spend_amount>0,reward_spend_amount,0))*100)::int as lifetime_awarded_moolah
    from reward
    group by 1,2 
)

,user_activity_joins as (
    select
        user.user_id
        ,user.user_type
        ,user.created_at_utc
        ,user_percentiles.user_id as order_user_id
        ,user_percentiles.most_recent_order as most_recent_paid_order_token
        ,zeroifnull(user_percentiles.total_completed_membership_orders) as total_completed_membership_orders
        ,zeroifnull(user_percentiles.total_paid_ala_carte_order_count) as total_paid_ala_carte_order_count
        ,zeroifnull(user_percentiles.total_paid_membership_order_count) as total_paid_membership_order_count
        ,zeroifnull(user_percentiles.last_90_days_paid_membership_order_count) as last_90_days_paid_membership_order_count
        ,zeroifnull(user_percentiles.lifetime_net_revenue) as lifetime_net_revenue
        ,zeroifnull(user_percentiles.lifetime_paid_order_count) as lifetime_paid_order_count
        ,zeroifnull(user_percentiles.total_completed_unpaid_uncancelled_orders) as total_completed_unpaid_uncancelled_orders
        ,zeroifnull(user_percentiles.total_paid_gift_order_count) as total_paid_gift_order_count
        ,zeroifnull(user_percentiles.six_month_net_revenue) as six_month_net_revenue
        ,zeroifnull(user_percentiles.six_month_gross_profit) as six_month_gross_profit
        ,zeroifnull(user_percentiles.twelve_month_net_revenue) as twelve_month_net_revenue
        ,zeroifnull(user_percentiles.six_month_paid_order_count) as six_month_paid_order_count
        ,zeroifnull(user_percentiles.twelve_month_purchase_count) as twelve_month_purchase_count
        ,zeroifnull(user_percentiles.last_90_days_paid_order_count) as last_90_days_paid_order_count
        ,zeroifnull(user_percentiles.last_180_days_paid_order_count) as last_180_days_paid_order_count
        ,zeroifnull(user_percentiles.recent_delivered_order_count) as recent_delivered_order_count
        ,zeroifnull(user_percentiles.six_month_net_revenue_percentile) as six_month_net_revenue_percentile
        ,zeroifnull(user_percentiles.six_month_gross_profit_percentile) as six_month_gross_profit_percentile
        ,zeroifnull(user_percentiles.twelve_month_net_revenue_percentile) as twelve_month_net_revenue_percentile
        ,zeroifnull(user_percentiles.lifetime_net_revenue_percentile) as lifetime_net_revenue_percentile
        ,zeroifnull(user_percentiles.total_california_orders) as total_california_orders
        ,zeroifnull(user_percentiles.user_average_order_value) as user_average_order_value
        ,zeroifnull(user_order_item_activity.lifetime_japanese_wagyu_revenue) as lifetime_japanese_wagyu_revenue
        ,zeroifnull(user_reward_activity.japanese_buyers_club_revenue) as japanese_buyers_club_revenue
        ,zeroifnull(user_reward_activity.moolah_points) as moolah_points
        ,zeroifnull(user_reward_activity.lifetime_awarded_moolah) as lifetime_awarded_moolah
        ,user_reward_activity.has_redeemed_moolah as has_redeemed_moolah
        ,average_order_days.average_order_frequency_days
        ,average_order_days.average_membership_order_frequency_days
        ,average_order_days.average_ala_carte_order_frequency_days
        ,coalesce(user_percentiles.is_rastellis,FALSE) as is_rastellis
        ,coalesce(user_percentiles.is_qvc,FALSE) as is_qvc
        ,user_percentiles.customer_cohort_date
        ,user_percentiles.membership_cohort_date
        ,user_percentiles.last_paid_membership_order_date
        ,user_percentiles.last_paid_ala_carte_order_date
        ,user_percentiles.last_paid_order_date
        ,user_percentiles.first_completed_order_date
        ,user_percentiles.first_completed_order_visit_id
    from user
        left join user_percentiles on user.user_id = user_percentiles.user_id
        left join average_order_days on user.user_id = average_order_days.user_id
        left join user_order_item_activity on user.user_id = user_order_item_activity.user_id
        left join user_reward_activity on user.user_id = user_reward_activity.user_id
)

select * from user_activity_joins

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
,discounts as (select * from {{ ref('discounts') }})

,user_order_activity as (
    select
        order_info.user_id
        ,is_rastellis
        ,is_qvc
        ,count(order_id) as total_order_count
        ,count_if(is_completed_order and is_membership_order) as total_completed_membership_orders
        ,count_if(is_paid_order and not is_cancelled_order and is_ala_carte_order) as total_paid_ala_carte_order_count
        ,count_if(is_paid_order and not is_cancelled_order and is_membership_order) total_paid_membership_order_count
        ,count_if(is_paid_order and not is_cancelled_order and is_membership_order and sysdate()::date - order_paid_at_utc::date <= 90) as last_90_days_paid_membership_order_count
        ,sum(iff(is_paid_order and not is_cancelled_order,net_revenue,0)) as lifetime_net_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order,net_product_revenue,0)) as lifetime_net_product_revenue
        ,count_if(is_paid_order and not is_cancelled_order) as lifetime_paid_order_count
        ,count_if(completed_order_rank = 1 and not is_paid_order and not is_cancelled_order) as total_completed_unpaid_uncancelled_orders
        ,count_if(is_gift_order and is_paid_order and not is_cancelled_order) as total_paid_gift_order_count
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-6,sysdate()),net_revenue,0)) as six_month_net_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-6,sysdate()),gross_profit,0)) as six_month_gross_profit
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-12,sysdate()),net_revenue,0)) as twelve_month_net_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-24,sysdate()),net_revenue,0)) as twentyfour_month_net_revenue
        ,count_if(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-6,sysdate())) as six_month_paid_order_count
        ,count_if(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-12,sysdate())) as twelve_month_purchase_count
        ,count_if(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-24,sysdate())) as twentyfour_purchase_count
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
        ,max(iff(is_paid_order and not is_cancelled_order,order_id,null)) as most_recent_order_id
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

-- CTE to rank discounts of the last orders, excluding specified promotions and buckets
,ranked_discounts AS (
    SELECT
        distinct
        lpo.user_id,
        d.promotion_id,
        --d.discount_usd,
        --d.order_id,
        RANK() OVER (
            PARTITION BY lpo.user_id 
            ORDER BY d.discount_usd DESC, d.promotion_id DESC
        ) AS rank
    FROM user_order_activity lpo
    JOIN discounts d ON lpo.most_recent_order_id = d.order_id
    WHERE 
       d.promotion_id NOT IN (104, 106, 226)
      AND d.revenue_waterfall_bucket NOT IN (
          'MOOLAH ITEM DISCOUNT', 'FREE SHIPPING DISCOUNT', 'FREE PROTEIN PROMOTION'
      )
      AND d.business_group NOT IN ('MEMBERSHIP 5%')
      AND d.promotion_source <> 'PROMOTION'
      
)


,user_percentiles as (
    select
        user_order_activity.*
        ,ntile(100) over(partition by six_month_net_revenue > 0 order by six_month_net_revenue) as six_month_net_revenue_percentile
        ,ntile(100) over(partition by six_month_paid_order_count > 0 order by six_month_gross_profit) as six_month_gross_profit_percentile
        ,ntile(100) over(partition by twelve_month_net_revenue > 0 order by twelve_month_net_revenue) as twelve_month_net_revenue_percentile
        ,ntile(100) over(partition by lifetime_net_revenue > 0 order by lifetime_net_revenue) as lifetime_net_revenue_percentile 
        ,ntile(100) over(partition by lifetime_paid_order_count > 0 order by lifetime_paid_order_count) as lifetime_paid_order_count_percentile 
        ,ranked_discounts.promotion_id as most_recent_order_promotion_id
    from user_order_activity
    left join ranked_discounts on ranked_discounts.user_id = user_order_activity.user_id and ranked_discounts.rank = 1 
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
        ,sum(iff(is_paid_order and not is_cancelled_order and order_paid_at_utc >= dateadd('month',-12,sysdate()),order_item_units.japanese_wagyu_revenue,0)) as twelve_month_japanese_wagyu_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.beef_revenue > 0, beef_revenue, false)) as beef_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.bison_revenue > 0, bison_revenue, false)) as bison_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.chicken_revenue > 0, chicken_revenue, false)) as chicken_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.japanese_wagyu_revenue > 0, japanese_wagyu_revenue, false)) as japanese_wagyu_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.lamb_revenue > 0, lamb_revenue, false)) as lamb_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.pork_revenue > 0, pork_revenue, false)) as pork_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.seafood_revenue > 0, seafood_revenue, false)) as seafood_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.starters_sides_revenue > 0, starters_sides_revenue, false)) as sides_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.turkey_revenue > 0, turkey_revenue, false)) as turkey_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.wagyu_revenue > 0, wagyu_revenue, false)) as wagyu_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.bundle_revenue > 0, bundle_revenue, false)) as bundle_revenue
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.beef_units > 0, order_paid_at_utc,0)) as most_recent_beef_order_date   
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.bison_units > 0, order_paid_at_utc,0)) as most_recent_bison_order_date 
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.chicken_units > 0, order_paid_at_utc,0)) as most_recent_chicken_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.japanese_wagyu_units > 0, order_paid_at_utc,0)) as most_recent_japanse_wagyu_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.lamb_units > 0, order_paid_at_utc,0)) as most_recent_lamb_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.pork_units > 0, order_paid_at_utc,0)) as most_recent_pork_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.seafood_units > 0, order_paid_at_utc,0)) as most_recent_seafood_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.starters_sides_units > 0, order_paid_at_utc,0)) as most_recent_sides_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.turkey_units > 0, order_paid_at_utc,0)) as most_recent_turkey_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.wagyu_units > 0, order_paid_at_utc,0)) as most_recent_wagyu_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.bundle_units > 0, order_paid_at_utc,0)) as most_recent_bundle_order_date
    from order_item_units
        inner join order_info on order_item_units.order_id = order_info.order_id
    group by 1
)

,user_reward_activity as (
    select
        user_id
        ,(sum(iff(rewards_program = 'MOOLAH' and reward_reason = 'REDEMPTION', reward_spend_amount,0))*100)::int as redeemed_moolah_points
        ,sum(iff(rewards_program = 'WAGYU_CLUB',reward_spend_amount,0)) as japanese_buyers_club_revenue
        ,(sum(iff(rewards_program = 'MOOLAH',reward_spend_amount,0))*100)::int as moolah_points
        ,(sum(iff(rewards_program = 'MOOLAH' and reward_spend_amount>0,reward_spend_amount,0))*100)::int as lifetime_awarded_moolah
    from reward
    group by 1
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
        ,zeroifnull(user_percentiles.lifetime_net_product_revenue) as lifetime_net_product_revenue
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
        ,zeroifnull(user_percentiles.lifetime_paid_order_count_percentile ) as lifetime_paid_order_count_percentile
        ,zeroifnull(user_percentiles.total_california_orders) as total_california_orders
        ,zeroifnull(user_percentiles.user_average_order_value) as user_average_order_value
        ,zeroifnull(user_order_item_activity.twelve_month_japanese_wagyu_revenue) as twelve_month_japanese_wagyu_revenue
        ,zeroifnull(user_order_item_activity.lifetime_japanese_wagyu_revenue) as lifetime_japanese_wagyu_revenue
        ,zeroifnull(user_reward_activity.japanese_buyers_club_revenue) as japanese_buyers_club_revenue
        ,zeroifnull(user_reward_activity.moolah_points) as moolah_points
        ,zeroifnull(user_reward_activity.lifetime_awarded_moolah) as lifetime_awarded_moolah
        ,zeroifnull(user_reward_activity.redeemed_moolah_points) as redeemed_moolah_points
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
        ,user_percentiles.most_recent_order_promotion_id
        ,user_percentiles.most_recent_order_id
        ,coalesce(first_completed_order_date is not null, FALSE) as purchaser
        ,coalesce(first_completed_order_date is null, FALSE) as all_leads 
        ,coalesce(first_completed_order_date is null and user.created_at_utc >= dateadd('day',-15,sysdate()), FALSE ) as hot_lead
        ,coalesce(first_completed_order_date is null and user.created_at_utc <= dateadd('day',-15,sysdate()) and user.created_at_utc >= dateadd('day',-45,sysdate()), FALSE ) as warm_lead
        ,coalesce(first_completed_order_date is null and user.created_at_utc <= dateadd('day',-45,sysdate()), FALSE ) as cold_lead
        ,coalesce(user_percentiles.last_paid_order_date >= dateadd('day',-90,sysdate()), FALSE) as recent_purchaser
        ,coalesce(user_percentiles.last_paid_order_date < dateadd('day',-90,sysdate()) and user_percentiles.last_paid_order_date >= dateadd('day',-180,sysdate()), FALSE) as lapsed_purchaser
        ,coalesce(user_percentiles.last_paid_order_date < dateadd('day',-180,sysdate()), FALSE) as dormant_purchaser
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.beef_revenue > 0, beef_revenue, false)) as beef_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.bison_revenue > 0, bison_revenue, false)) as bison_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.chicken_revenue > 0, chicken_revenue, false)) as chicken_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.japanese_wagyu_revenue > 0, japanese_wagyu_revenue, false)) as japanese_wagyu_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.lamb_revenue > 0, lamb_revenue, false)) as lamb_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.pork_revenue > 0, pork_revenue, false)) as pork_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.seafood_revenue > 0, seafood_revenue, false)) as seafood_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.starters_sides_revenue > 0, starters_sides_revenue, false)) as sides_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.turkey_revenue > 0, turkey_revenue, false)) as turkey_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.wagyu_revenue > 0, wagyu_revenue, false)) as wagyu_revenue
        ,sum(iff(is_paid_order and not is_cancelled_order and order_items_units.bundle_revenue > 0, bundle_revenue, false)) as bundle_revenue
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.beef_units > 0, order_paid_at_utc,0)) as most_recent_beef_order_date   
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.bison_units > 0, order_paid_at_utc,0)) as most_recent_bison_order_date 
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.chicken_units > 0, order_paid_at_utc,0)) as most_recent_chicken_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.japanese_wagyu_units > 0, order_paid_at_utc,0)) as most_recent_japanse_wagyu_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.lamb_units > 0, order_paid_at_utc,0)) as most_recent_lamb_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.pork_units > 0, order_paid_at_utc,0)) as most_recent_pork_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.seafood_units > 0, order_paid_at_utc,0)) as most_recent_seafood_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.starters_sides_units > 0, order_paid_at_utc,0)) as most_recent_sides_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.turkey_units > 0, order_paid_at_utc,0)) as most_recent_turkey_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.wagyu_units > 0, order_paid_at_utc,0)) as most_recent_wagyu_order_date
        ,max(iff(is_paid_order and not is_cancelled_order and order_item_units.bundle_units > 0, order_paid_at_utc,0)) as most_recent_bundle_order_date
        
    from user
        left join user_percentiles on user.user_id = user_percentiles.user_id
        left join average_order_days on user.user_id = average_order_days.user_id
        left join user_order_item_activity on user.user_id = user_order_item_activity.user_id
        left join user_reward_activity on user.user_id = user_reward_activity.user_id
)

select * from user_activity_joins

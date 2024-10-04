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
,promotions as (select * from {{ ref('promotions')  }})
,promotions_promotions as (select * from {{ ref('promotions_promotions') }})


,user_order_activity as (
    select
        order_info.user_id
        ,is_rastellis
        ,is_qvc
        ,count(order_id) as total_order_count
        ,countif(is_completed_order and is_membership_order) as total_completed_membership_orders
        ,countif(is_paid_order and not is_cancelled_order and is_ala_carte_order) as total_paid_ala_carte_order_count
        ,countif(is_paid_order and not is_cancelled_order and is_membership_order) total_paid_membership_order_count
        ,countif(is_paid_order and not is_cancelled_order and is_membership_order and DATE_DIFF(CURRENT_DATE(), CAST(order_paid_at_utc AS DATE), DAY) <= 90) as last_90_days_paid_membership_order_count
        ,sum(if(is_paid_order and not is_cancelled_order,net_revenue,0)) as lifetime_net_revenue
        ,sum(if(is_paid_order and not is_cancelled_order,net_product_revenue,0)) as lifetime_net_product_revenue
        ,countif(is_paid_order and not is_cancelled_order) as lifetime_paid_order_count
        ,countif(completed_order_rank = 1 and not is_paid_order and not is_cancelled_order) as total_completed_unpaid_uncancelled_orders
        ,countif(is_gift_order and is_paid_order and not is_cancelled_order) as total_paid_gift_order_count
        ,sum(if(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 6 MONTH) ,net_revenue,0)) as six_month_net_revenue
        ,sum(if(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 6 MONTH) ,gross_profit,0)) as six_month_gross_profit
        ,sum(if(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 12 MONTH) ,net_revenue,0)) as twelve_month_net_revenue
        ,sum(if(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 24 MONTH) ,net_revenue,0)) as twentyfour_month_net_revenue
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 6 MONTH) ) as six_month_paid_order_count
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 12 MONTH) ) as twelve_month_purchase_count
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 24 MONTH) ) as twentyfour_purchase_count
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 30 DAY) ) as last_30_days_paid_order_count
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 60 DAY) ) as last_60_days_paid_order_count
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 90 DAY) ) as last_90_days_paid_order_count
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 120 DAY) ) as last_120_days_paid_order_count
        ,countif(is_paid_order and not is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 180 DAY) ) as last_180_days_paid_order_count
        ,countif(has_been_delivered and cast(delivered_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 7 DAY)) as recent_delivered_order_count
        ,min(if(is_paid_order and not is_cancelled_order,cast(order_paid_at_utc as date),null)) as customer_cohort_date
        ,min(if(is_paid_order and not is_cancelled_order and is_membership_order,cast(order_paid_at_utc as date),null)) as membership_cohort_date
        ,max(if(is_paid_order and not is_cancelled_order and is_membership_order,cast(order_paid_at_utc as date),null)) as last_paid_membership_order_date
        ,max(if(is_paid_order and not is_cancelled_order and is_ala_carte_order,cast(order_paid_at_utc as date),null)) as last_paid_ala_carte_order_date
        ,max(if(is_paid_order and not is_cancelled_order,cast(order_paid_at_utc as date),null)) as last_paid_order_date
        ,min(if(completed_order_rank = 1,order_id,null)) as first_completed_order_id
        ,min(if(completed_order_rank = 1,order_checkout_completed_at_utc,null)) as first_completed_order_date
        ,min(if(completed_order_rank = 1,visit_id,null)) as first_completed_order_visit_id
        ,max(if(is_paid_order and not is_cancelled_order,order_token,null)) as most_recent_order
        ,max(if(is_paid_order and not is_cancelled_order,order_id,null)) as most_recent_order_id
        ,max(if(is_paid_order and not is_cancelled_order and is_moolah_order,cast(order_paid_at_utc as date),null)) as last_paid_moolah_order_date
        ,countif(is_customer_impactful_reschedule and cast(order_reschedule_occurred_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 14 DAY)) as last_14_days_impacful_customer_reschedules

        ,countif(
            not is_gift_order
            and not is_gift_card_order
            and not is_bulk_gift_order
            and not is_cancelled_order
            and is_paid_order
            and (billing_state = 'CA' or order_delivery_state = 'CA')
         ) as total_california_orders
        ,avg(if(is_paid_order and not is_cancelled_order,net_revenue,null)) as user_average_order_value
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
        
        ,lead(case when paid_order_rank is not null then cast(order_paid_at_utc as date) end,1) 
            over (partition by user_id order by paid_order_rank)  - cast(order_paid_at_utc as date) as days_to_next_paid_order
        
        ,lead(case when paid_membership_order_rank is not null then cast(order_paid_at_utc as date) end,1) 
            over (partition by user_id order by paid_membership_order_rank)  - cast(order_paid_at_utc as date) as days_to_next_paid_membership_order
        
        ,lead(case when paid_ala_carte_order_rank is not null then cast(order_paid_at_utc as date) end,1) 
            over (partition by user_id order by paid_ala_carte_order_rank)  - cast(order_paid_at_utc as date) as days_to_next_paid_ala_carte_order
    from order_info
)

,last_paid_order_info as (
    select
        user_order_activity.user_id as user_id
        ,order_info.net_revenue as last_paid_order_value
    from user_order_activity
    left join order_info on user_order_activity.most_recent_order_id = order_info.order_id
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

,promotion_rank as (
    select 
        order_id,
        discounts.promotion_id,
        discounts.promotion_source,
        case when discounts.promotion_source = 'PROMOTION' then promotions.promotion_type else promotions_promotions.name end as promotion_name, 
        discount_usd,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount_usd DESC) AS rank
    from discounts
    left join promotions on discounts.promotion_id = promotions.promotion_id and discounts.promotion_source = 'PROMOTION' 
    left join promotions_promotions on discounts.promotion_id = promotions_promotions.id and discounts.promotion_source = 'PROMOTIONS::PROMOTION'
    where discounts.promotion_id is not null
    and discounts.promotion_id not in (3,10,14,275,104,226) --removing free shipping and in-cart specials
)

,user_order_item_activity as (
    select
        order_info.user_id
        
        ,sum(
            if(
                order_info.is_paid_order 
                and not order_info.is_cancelled_order
                ,order_item_units.japanese_wagyu_revenue
                ,0
            )
        ) as lifetime_japanese_wagyu_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and cast(order_paid_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 12 MONTH),order_item_units.japanese_wagyu_revenue,0)) as twelve_month_japanese_wagyu_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.beef_revenue > 0, beef_revenue, 0)) as beef_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.bison_revenue > 0, bison_revenue, 0)) as bison_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.chicken_revenue > 0, chicken_revenue, 0)) as chicken_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.japanese_wagyu_revenue > 0, japanese_wagyu_revenue, 0)) as japanese_wagyu_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.lamb_revenue > 0, lamb_revenue, 0)) as lamb_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.pork_revenue > 0, pork_revenue, 0)) as pork_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.seafood_revenue > 0, seafood_revenue, 0)) as seafood_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.starters_sides_revenue > 0, starters_sides_revenue, 0)) as sides_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.turkey_revenue > 0, turkey_revenue, 0)) as turkey_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.wagyu_revenue > 0, wagyu_revenue, 0)) as wagyu_revenue
        ,sum(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.bundle_revenue > 0, bundle_revenue, 0)) as bundle_revenue
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.beef_units > 0, order_paid_at_utc,null)) as most_recent_beef_order_date   
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.bison_units > 0, order_paid_at_utc,null)) as most_recent_bison_order_date 
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.chicken_units > 0, order_paid_at_utc,null)) as most_recent_chicken_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.japanese_wagyu_units > 0, order_paid_at_utc,null)) as most_recent_japanse_wagyu_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.lamb_units > 0, order_paid_at_utc,null)) as most_recent_lamb_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.pork_units > 0, order_paid_at_utc,null)) as most_recent_pork_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.seafood_units > 0, order_paid_at_utc,null)) as most_recent_seafood_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.starters_sides_units > 0, order_paid_at_utc,null)) as most_recent_sides_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.turkey_units > 0, order_paid_at_utc,null)) as most_recent_turkey_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.wagyu_units > 0, order_paid_at_utc,null)) as most_recent_wagyu_order_date
        ,max(if(order_info.is_paid_order and not order_info.is_cancelled_order and order_item_units.bundle_units > 0, order_paid_at_utc,null)) as most_recent_bundle_order_date
    from order_item_units
        inner join order_info on order_item_units.order_id = order_info.order_id
    group by 1
)

,user_reward_activity as (
    select
        user_id
        ,cast((sum(if(rewards_program = 'MOOLAH' and reward_reason = 'REDEMPTION', reward_spend_amount,0))*100) as int64) as redeemed_moolah_points
        ,sum(if(rewards_program = 'WAGYU_CLUB',reward_spend_amount,0)) as japanese_buyers_club_revenue
        ,cast((sum(if(rewards_program = 'MOOLAH',reward_spend_amount,0))*100) as int64) as moolah_points
        ,cast((sum(if(rewards_program = 'MOOLAH' and reward_spend_amount>0,reward_spend_amount,0))*100) as int64) as lifetime_awarded_moolah
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
        ,coalesce(user_percentiles.total_completed_membership_orders) as total_completed_membership_orders
        ,coalesce(user_percentiles.total_paid_ala_carte_order_count) as total_paid_ala_carte_order_count
        ,coalesce(user_percentiles.total_paid_membership_order_count) as total_paid_membership_order_count
        ,coalesce(user_percentiles.last_90_days_paid_membership_order_count) as last_90_days_paid_membership_order_count
        ,coalesce(user_percentiles.lifetime_net_revenue) as lifetime_net_revenue
        ,coalesce(user_percentiles.lifetime_net_product_revenue) as lifetime_net_product_revenue
        ,coalesce(user_percentiles.lifetime_paid_order_count) as lifetime_paid_order_count
        ,coalesce(user_percentiles.total_completed_unpaid_uncancelled_orders) as total_completed_unpaid_uncancelled_orders
        ,coalesce(user_percentiles.total_paid_gift_order_count) as total_paid_gift_order_count
        ,coalesce(user_percentiles.six_month_net_revenue) as six_month_net_revenue
        ,coalesce(user_percentiles.six_month_gross_profit) as six_month_gross_profit
        ,coalesce(user_percentiles.twelve_month_net_revenue) as twelve_month_net_revenue
        ,coalesce(user_percentiles.six_month_paid_order_count) as six_month_paid_order_count
        ,coalesce(user_percentiles.twelve_month_purchase_count) as twelve_month_purchase_count
        ,coalesce(user_percentiles.last_30_days_paid_order_count) as last_30_days_paid_order_count
        ,coalesce(user_percentiles.last_60_days_paid_order_count) as last_60_days_paid_order_count
        ,coalesce(user_percentiles.last_90_days_paid_order_count) as last_90_days_paid_order_count
        ,coalesce(user_percentiles.last_120_days_paid_order_count) as last_120_days_paid_order_count
        ,coalesce(user_percentiles.last_180_days_paid_order_count) as last_180_days_paid_order_count
        ,coalesce(user_percentiles.recent_delivered_order_count) as recent_delivered_order_count
        ,coalesce(user_percentiles.six_month_net_revenue_percentile) as six_month_net_revenue_percentile
        ,coalesce(user_percentiles.six_month_gross_profit_percentile) as six_month_gross_profit_percentile
        ,coalesce(user_percentiles.twelve_month_net_revenue_percentile) as twelve_month_net_revenue_percentile
        ,coalesce(user_percentiles.lifetime_net_revenue_percentile) as lifetime_net_revenue_percentile
        ,coalesce(user_percentiles.lifetime_paid_order_count_percentile ) as lifetime_paid_order_count_percentile
        ,coalesce(user_percentiles.total_california_orders) as total_california_orders
        ,coalesce(user_percentiles.user_average_order_value) as user_average_order_value
        ,coalesce(user_order_item_activity.twelve_month_japanese_wagyu_revenue) as twelve_month_japanese_wagyu_revenue
        ,coalesce(user_order_item_activity.lifetime_japanese_wagyu_revenue) as lifetime_japanese_wagyu_revenue
        ,coalesce(user_reward_activity.japanese_buyers_club_revenue) as japanese_buyers_club_revenue
        ,coalesce(user_reward_activity.moolah_points) as moolah_points
        ,coalesce(user_reward_activity.lifetime_awarded_moolah) as lifetime_awarded_moolah
        ,coalesce(user_reward_activity.redeemed_moolah_points) as redeemed_moolah_points
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
        ,user_percentiles.first_completed_order_id
        ,user_percentiles.first_completed_order_date
        ,user_percentiles.first_completed_order_visit_id
        ,user_percentiles.most_recent_order_promotion_id
        ,user_percentiles.most_recent_order_id
        ,promotion_rank.promotion_id as acquisition_promotion_id
        ,promotion_rank.promotion_source as acquisition_promotion_source
        ,promotion_rank.promotion_name as acquisition_promotion_name
        ,coalesce(first_completed_order_date is not null, FALSE) as purchaser
        ,coalesce(first_completed_order_date is null, FALSE) as all_leads 
        ,coalesce(first_completed_order_date is null and cast(user.created_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 15 DAY), FALSE ) as hot_lead
        ,coalesce(first_completed_order_date is null and cast(user.created_at_utc as date) <= DATE_SUB(current_date(),INTERVAL 15 DAY) and cast(user.created_at_utc as date) >= DATE_SUB(current_date(),INTERVAL 45 DAY), FALSE ) as warm_lead
        ,coalesce(first_completed_order_date is null and cast(user.created_at_utc as date) <= DATE_SUB(current_date(),INTERVAL 45 DAY), FALSE ) as cold_lead
        ,coalesce(cast(user_percentiles.last_paid_order_date as date) >= DATE_SUB(current_date(),INTERVAL 90 DAY), FALSE) as recent_purchaser
        ,coalesce(cast(user_percentiles.last_paid_order_date as date) < DATE_SUB(current_date(),INTERVAL 90 DAY) and cast(user_percentiles.last_paid_order_date as date) >= DATE_SUB(current_date(),INTERVAL 60 DAY), FALSE) as lapsed_purchaser
        ,coalesce(cast(user_percentiles.last_paid_order_date as date) < DATE_SUB(current_date(),INTERVAL 180 DAY), FALSE) as dormant_purchaser
        ,beef_revenue
        ,bison_revenue
        ,chicken_revenue
        ,japanese_wagyu_revenue
        ,lamb_revenue
        ,pork_revenue
        ,sides_revenue
        ,turkey_revenue
        ,wagyu_revenue
        ,bundle_revenue
        ,seafood_revenue
        ,most_recent_beef_order_date   
        ,most_recent_bison_order_date 
        ,most_recent_chicken_order_date
        ,most_recent_japanse_wagyu_order_date
        ,most_recent_lamb_order_date
        ,most_recent_pork_order_date
        ,most_recent_seafood_order_date
        ,most_recent_sides_order_date
        ,most_recent_turkey_order_date
        ,most_recent_wagyu_order_date
        ,most_recent_bundle_order_date
        ,last_paid_order_info.last_paid_order_value
        ,last_paid_moolah_order_date
        ,last_14_days_impacful_customer_reschedules
        
    from user
        left join user_percentiles on user.user_id = user_percentiles.user_id
        left join average_order_days on user.user_id = average_order_days.user_id
        left join user_order_item_activity on user.user_id = user_order_item_activity.user_id
        left join user_reward_activity on user.user_id = user_reward_activity.user_id
        left join last_paid_order_info on user.user_id = last_paid_order_info.user_id
        left join promotion_rank on user_percentiles.first_completed_order_id = promotion_rank.order_id and promotion_rank.rank = 1
)

select * from user_activity_joins

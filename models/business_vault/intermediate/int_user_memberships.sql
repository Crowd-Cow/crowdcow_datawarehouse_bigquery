with

membership as ( select * from {{ ref('stg_cc__subscriptions') }} where user_id is not null )
,membership_promo as ( select * from {{ ref('stg_cc__subscription_promotions') }} )
,promotion as ( select * from {{ ref('stg_cc__promotions') }} where dbt_valid_to is null )

,get_membership_history as (
    select distinct
        user_id
        ,first_value(subscription_created_at_utc) over(partition by user_id order by subscription_created_at_utc) as first_membership_created_date
        ,last_value(subscription_created_at_utc) over(partition by user_id order by subscription_created_at_utc) as most_recent_membership_created_date
        ,last_value(subscription_cancelled_at_utc) over(partition by user_id order by subscription_created_at_utc) as most_recent_membership_cancelled_date
        ,first_value(subscription_id) over(partition by user_id order by subscription_created_at_utc) as first_membership_id
        ,last_value(subscription_id) over(partition by user_id order by subscription_created_at_utc) as current_membership_id
        ,last_value(subscription_renew_period_type) over(partition by user_id order by subscription_created_at_utc) as current_renew_period
        ,count(subscription_id) over(partition by user_id) as total_membership_count
        ,count(iff(not is_uncancelled_membership,subscription_id,null)) over(partition by user_id) as total_cancelled_membership_count
    from membership
)

,get_promotion_history as (
    select distinct
        subscription_id
        ,first_value(coalesce(promotion_selection_id,promotion_id)) over(partition by subscription_id order by created_at_utc) as first_promotion_id
        ,last_value(coalesce(promotion_selection_id,promotion_id)) over(partition by subscription_id order by created_at_utc) as current_promotion_id
    from membership_promo
)

,calc_membership_tenure as (
    select
        *
        ,datediff(month,first_membership_created_date,coalesce(most_recent_membership_cancelled_date,sysdate())) as membership_tenure_months
    from get_membership_history
)

,get_current_membership_promotion as (
    select
        calc_membership_tenure.*
        ,get_promotion_history.current_promotion_id
        ,promotion.promotion_type as current_promotion_type
    from calc_membership_tenure
        left join get_promotion_history on calc_membership_tenure.current_membership_id = get_promotion_history.subscription_id
        left join promotion on get_promotion_history.current_promotion_id = promotion.promotion_id
)

,get_first_membership_promotion as (
    select
        get_current_membership_promotion.*
        ,get_promotion_history.first_promotion_id
        ,promotion.promotion_type as first_promotion_type
    from get_current_membership_promotion
        left join get_promotion_history on get_current_membership_promotion.first_membership_id = get_promotion_history.subscription_id
        left join promotion on get_promotion_history.first_promotion_id = promotion.promotion_id
)

select
    user_id
    ,first_membership_id
    ,current_membership_id
    ,first_promotion_id
    ,current_promotion_id
    ,first_promotion_type
    ,current_promotion_type
    ,current_renew_period
    ,total_membership_count
    ,total_cancelled_membership_count
    ,total_membership_count - total_cancelled_membership_count as total_uncancelled_memberships
    ,membership_tenure_months
    ,first_membership_created_date
    ,most_recent_membership_created_date
    ,most_recent_membership_cancelled_date
from get_first_membership_promotion

with

membership as ( select * from {{ ref('stg_cc__subscriptions') }} where user_id is not null )
,membership_promo as ( select * from {{ ref('stg_cc__subscription_promotions') }} )
,promotion as ( select * from {{ ref('stg_cc__promotions') }} where dbt_valid_to is null and promotion_source = 'PROMOTION' )
,promotions_promotions as ( select * from {{ ref('stg_cc__promotions_promotions') }})

,get_membership_history as (
    select distinct
        user_id
        ,FIRST_VALUE(subscription_created_at_utc) OVER (PARTITION BY user_id ORDER BY subscription_created_at_utc) AS first_membership_created_date
        ,LAST_VALUE(subscription_created_at_utc) OVER (PARTITION BY user_id ORDER BY subscription_created_at_utc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS most_recent_membership_created_date
        ,LAST_VALUE(subscription_cancelled_at_utc) OVER (PARTITION BY user_id ORDER BY subscription_created_at_utc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS most_recent_membership_cancelled_date
        ,FIRST_VALUE(subscription_id) OVER (PARTITION BY user_id ORDER BY subscription_created_at_utc) AS first_membership_id
        ,LAST_VALUE(subscription_id) OVER (PARTITION BY user_id ORDER BY subscription_created_at_utc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS current_membership_id
        ,LAST_VALUE(subscription_renew_period_type) OVER (PARTITION BY user_id ORDER BY subscription_created_at_utc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS current_renew_period
        ,COUNT(subscription_id) OVER (PARTITION BY user_id) AS total_membership_count
        ,COUNTIF(NOT cast(is_uncancelled_membership as bool)) OVER (PARTITION BY user_id) AS total_cancelled_membership_count
    from membership
)

,get_promotion_history as (
    select distinct
        subscription_id
        ,first_value(coalesce(promotion_selection_id,promotion_id)) over(partition by subscription_id order by created_at_utc) as first_promotion_id
        ,last_value(coalesce(promotion_selection_id,promotion_id)) over(partition by subscription_id order by created_at_utc RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as current_promotion_id
    from membership_promo
)

,calc_membership_tenure as (
    select
        *
        ,DATE_DIFF(COALESCE(CAST(most_recent_membership_cancelled_date AS DATE), CURRENT_DATE()),CAST(first_membership_created_date AS DATE),MONTH) AS membership_tenure_months

    from get_membership_history
)

,get_current_membership_promotion as (
    select
        calc_membership_tenure.*
        ,get_promotion_history.current_promotion_id
        ,coalesce(promotion.promotion_type,promotions_promotions.name) as current_promotion_type
    from calc_membership_tenure
        left join get_promotion_history on calc_membership_tenure.current_membership_id = get_promotion_history.subscription_id
        left join promotion on get_promotion_history.current_promotion_id = promotion.promotion_id
        left join promotions_promotions on get_promotion_history.current_promotion_id = promotions_promotions.id
)

,get_first_membership_promotion as (
    select
        get_current_membership_promotion.*
        ,get_promotion_history.first_promotion_id
        ,coalesce(promotion.promotion_type,promotions_promotions.name) as first_promotion_type
    from get_current_membership_promotion
        left join get_promotion_history on get_current_membership_promotion.first_membership_id = get_promotion_history.subscription_id
        left join promotion on get_promotion_history.first_promotion_id = promotion.promotion_id
        left join promotions_promotions on get_promotion_history.first_promotion_id = promotions_promotions.id
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
    ,first_promotion_type like '%LIFETIME%' as is_first_promotion_ffl
    ,current_promotion_type like '%LIFETIME%' as is_current_promotion_ffl 
    ,first_membership_created_date
    ,most_recent_membership_created_date
    ,most_recent_membership_cancelled_date
from get_first_membership_promotion

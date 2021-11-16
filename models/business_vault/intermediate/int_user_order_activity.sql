with

user as ( select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null )
,order_info as ( select * from {{ ref('orders') }} )


,order_count as (
    select
        user_id
        ,count(order_id) as total_order_count
        ,count_if(is_paid_order and not is_cancelled_order and is_ala_carte_order) as total_paid_ala_carte_order_count
        ,count_if(is_paid_order and not is_cancelled_order and is_membership_order) total_paid_membership_order_count
        ,count_if(is_paid_order and not is_cancelled_order and is_membership_order and sysdate()::date - order_paid_at_utc::date <= 90) as total_active_order_count
    from order_info
    group by 1
)

,order_cohorts as (
    select distinct
        user_id
        ,first_value(order_paid_at_utc::date) over(partition by user_id order by paid_order_rank) as customer_cohort_date
        ,first_value(order_paid_at_utc::date) over(partition by user_id order by paid_membership_order_rank) as membership_cohort_date
    from order_info
)

,order_frequency as (
    select
        user_id
        ,lead(case when paid_order_rank is not null then order_paid_at_utc::date end,1) over (partition by user_id order by paid_order_rank)  - order_paid_at_utc::date as days_to_next_paid_order
        ,lead(case when paid_membership_order_rank is not null then order_paid_at_utc::date end,1) over (partition by user_id order by paid_membership_order_rank)  - order_paid_at_utc::date as days_to_next_paid_membership_order
        ,lead(case when paid_ala_carte_order_rank is not null then order_paid_at_utc::date end,1) over (partition by user_id order by paid_ala_carte_order_rank)  - order_paid_at_utc::date as days_to_next_paid_ala_carte_order
    from order_info
)

,average_order_days as (
    select
        user_id
        ,avg(days_to_next_paid_order) as average_order_frequency_days
        ,avg(days_to_next_paid_membership_order) as average_membership_order_frequency_days
        ,avg(days_to_next_paid_ala_carte_order) as average_ala_carte_order_frequncy_days
    from order_frequency
    group by 1
)

,user_activity_joins as (
    select
        user.user_id
        ,order_count.user_id as order_user_id
        ,zeroifnull(order_count.total_paid_ala_carte_order_count) as total_paid_ala_carte_order_count
        ,zeroifnull(order_count.total_paid_membership_order_count) as total_paid_membership_order_count
        ,zeroifnull(order_count.total_active_order_count) as total_active_order_count
        ,average_order_frequency_days
        ,average_membership_order_frequency_days
        ,average_ala_carte_order_frequncy_days
        ,order_cohorts.customer_cohort_date
        ,order_cohorts.membership_cohort_date
    from user
        left join order_count on user.user_id = order_count.user_id
        left join order_cohorts on user.user_id = order_cohorts.user_id
        left join average_order_days on user.user_id = average_order_days.user_id
)

select * from user_activity_joins
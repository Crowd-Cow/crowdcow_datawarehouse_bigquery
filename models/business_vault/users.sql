with

users as (select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null)
,memberships as (select * from {{ ref('stg_cc__subscriptions') }})
,order_info as (select * from {{ ref('orders') }})

,membership_count as (
    select
        user_id
        ,count(subscription_id) as total_membership_count
        ,count_if(not is_uncancelled_membership) as total_cancelled_membership_count
        ,count(subscription_id) - count_if(not is_uncancelled_membership) as total_uncancelled_memberships
    from memberships
    group by 1
)

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

,user_joins as (
    select
        users.*
        ,membership_count.user_id is not null as is_member
        ,membership_count.user_id is not null and membership_count.total_uncancelled_memberships = 0 as is_cancelled_member
        ,order_count.user_id is null as is_lead
        ,order_count.user_id is not null and total_paid_ala_carte_order_count > 0 as is_purchasing_customer
        ,order_count.user_id is not null and total_paid_membership_order_count > 0 as is_purchasing_member
        ,order_count.user_id is not null and total_active_order_count > 0 as is_active_member
        ,order_cohorts.customer_cohort_date
        ,order_cohorts.membership_cohort_date
    from users
        left join membership_count on users.user_id = membership_count.user_id
        left join order_count on users.user_id = order_count.user_id
        left join order_cohorts on users.user_id = order_cohorts.user_id
)

,final as (
    select
        user_id
        ,phone_number_id
        
        ,case
            when user_roles_for_access is not null then 'EMPLOYEE'
            when user_email like '%@CROWDCOW.COM' and user_email not like 'TEMPORARY%CROWDCOW.COM' then 'INTERNAL'
            when user_email like 'TEMPORARY%CROWDCOW.COM' then 'GUEST'
            when user_type = 'C' then 'CUSTOMER'
            when user_type = 'P' then 'PLACEHOLDER'
            else 'OTHER'
         end as user_type
        
        ,user_gender
        ,user_email_name
        ,user_email
        ,user_roles_for_access
        ,user_zip
        ,user_token
        ,user_referrer_token
        ,user_cow_cash_balance_usd
        ,user_support_status
        ,customer_cohort_date
        ,membership_cohort_date
        ,is_member
        ,is_cancelled_member
        ,is_lead
        ,is_purchasing_customer
        ,is_purchasing_member
        ,is_active_member
        ,user_last_sign_in_at_utc
        ,created_at_utc
        ,updated_at_utc
    from user_joins
)

select * from final
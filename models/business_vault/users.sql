with

users as (select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null)
,user_order_activity as ( select * from {{ ref('int_user_order_activity') }} )
,memberships as (select * from {{ ref('stg_cc__subscriptions') }})

,membership_count as (
    select
        user_id
        ,count(subscription_id) as total_membership_count
        ,count_if(not is_uncancelled_membership) as total_cancelled_membership_count
        ,count(subscription_id) - count_if(not is_uncancelled_membership) as total_uncancelled_memberships
    from memberships
    group by 1
)

,user_joins as (
    select
        users.*
        ,membership_count.user_id is not null as is_member
        ,membership_count.user_id is not null and membership_count.total_uncancelled_memberships = 0 as is_cancelled_member
        ,user_order_activity.order_user_id is null as is_lead
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_ala_carte_order_count > 0 and membership_count.total_membership_count = 0 as is_purchasing_customer
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_membership_order_count > 0 as is_purchasing_member
        ,user_order_activity.user_id is not null and user_order_activity.total_active_order_count > 0 as is_active_member
        ,coalesce(sysdate()::date - user_order_activity.last_paid_ala_carte_order_date > 45,FALSE) as is_ala_carte_attrition_risk
        ,user_order_activity.average_order_frequency_days
        ,user_order_activity.average_membership_order_frequency_days
        ,user_order_activity.average_ala_carte_order_frequncy_days
        ,user_order_activity.customer_cohort_date
        ,user_order_activity.membership_cohort_date
    from users
        left join membership_count on users.user_id = membership_count.user_id
        left join user_order_activity on users.user_id = user_order_activity.user_id
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
        ,average_order_frequency_days
        ,average_membership_order_frequency_days
        ,average_ala_carte_order_frequncy_days
        ,is_member
        ,is_cancelled_member
        ,is_lead
        ,is_purchasing_customer
        ,is_purchasing_member
        ,is_active_member
        ,is_ala_carte_attrition_risk
        ,user_last_sign_in_at_utc
        ,created_at_utc
        ,updated_at_utc
    from user_joins
)

select * from final
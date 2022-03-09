with

users as (select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null)
,user_order_activity as ( select * from {{ ref('int_user_order_activity') }} )
,memberships as (select * from {{ ref('stg_cc__subscriptions') }})
,phone_number as ( select * from {{ ref('stg_cc__phone_numbers') }} )
,ccpa_users as ( select distinct  user_token from {{ ref('ccpa_requests') }} )
,visit as ( select * from {{ ref('base_cc__ahoy_visits') }} )
,postal_code as ( select * from {{ ref('stg_cc__postal_codes') }} )

,user_visits as (
    select
        user_id
        ,first_value(visit_id) over(partition by user_id order by started_at_utc, visit_id) as first_visit_id
    from visit
    where user_id is not null
    qualify row_number() over(partition by user_id order by started_at_utc, visit_id) = 1
)

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
        ,phone_number.phone_type
        ,phone_number.phone_number
        ,phone_number.does_allow_sms
        ,membership_count.user_id is not null and total_completed_membership_orders > 0 as is_member
        ,membership_count.user_id is not null and membership_count.total_uncancelled_memberships = 0 as is_cancelled_member
        ,user_order_activity.order_user_id is null as is_lead
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_ala_carte_order_count > 0 and membership_count.total_membership_count = 0 as is_purchasing_customer
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_membership_order_count > 0 as is_purchasing_member
        ,user_order_activity.user_id is not null and user_order_activity.total_active_90_day_order_count > 0 as is_active_member_90_day
        
        ,case
            when user_order_activity.customer_cohort_date < user_order_activity.membership_cohort_date then membership_cohort_date - customer_cohort_date
         end as days_from_ala_carte_to_membership

        ,user_order_activity.average_order_frequency_days
        ,user_order_activity.average_membership_order_frequency_days
        ,user_order_activity.average_ala_carte_order_frequency_days
        ,user_order_activity.customer_cohort_date
        ,user_order_activity.membership_cohort_date
        ,user_order_activity.first_completed_order_date
        ,user_order_activity.first_completed_order_visit_id
        ,user_visits.first_visit_id

        ,case
            when user_order_activity.first_completed_order_date::date - users.created_at_utc::date <= 10 then user_visits.first_visit_id
            else user_order_activity.first_completed_order_visit_id
         end as attributed_visit_id

        ,ccpa_users.user_token is not null as is_ccpa
        ,users.user_banned_at_utc is not null as is_banned
        ,postal_code.state_code
        ,postal_code.city_name

    from users
        left join membership_count on users.user_id = membership_count.user_id
        left join user_order_activity on users.user_id = user_order_activity.user_id
        left join phone_number on users.phone_number_id = phone_number.phone_number_id
        left join aggregate_tags on users.user_id = aggregate_tags.user_id
        left join ccpa_users on users.user_token = ccpa_users.user_token
        left join user_visits on users.user_id = user_visits.user_id
        left join postal_code on users.user_zip = postal_code.postal_code
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
        ,phone_number
        ,phone_type
        ,user_roles_for_access
        ,state_code
        ,city_name
        ,user_zip
        ,user_token
        ,user_referrer_token
        ,user_cow_cash_balance_usd
        ,user_support_status
        ,customer_cohort_date
        ,membership_cohort_date
        ,average_order_frequency_days
        ,average_membership_order_frequency_days
        ,average_ala_carte_order_frequency_days
        ,days_from_ala_carte_to_membership
        ,attributed_visit_id
        ,is_member
        ,is_cancelled_member
        ,is_lead
        ,is_purchasing_customer
        ,is_purchasing_member
        ,is_active_member_90_day
        ,does_allow_sms
        ,user_last_sign_in_at_utc
        ,created_at_utc
        ,updated_at_utc
        ,is_banned
    from user_joins
    where not is_ccpa
)

select * from final

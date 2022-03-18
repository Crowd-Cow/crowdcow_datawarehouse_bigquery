with

users as (select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null)
,user_order_activity as ( select * from {{ ref('int_user_order_activity') }} )
,memberships as (select * from {{ ref('stg_cc__subscriptions') }})
,phone_number as ( select * from {{ ref('stg_cc__phone_numbers') }} )
,ccpa_users as ( select distinct  user_token from {{ ref('ccpa_requests') }} )
,visit as ( select * from {{ ref('base_cc__ahoy_visits') }} )
,postal_code as ( select * from {{ ref('stg_cc__postal_codes') }} )
,identity as ( select * from {{ ref('int_recent_user_identities') }} )
,contact as ( select * from {{ ref('stg_pb__contacts') }} )

,user_contacts as (
    select 
        user_token
        ,last_call_at_utc
        ,total_calls
        ,call_result
    from contact
    where user_token is not null
    qualify row_number() over(partition by user_token order by date_modified_at_utc desc) = 1
)

,most_recent_membership as (
    select
        user_id
        ,subscription_id
        ,subscription_created_at_utc as most_recent_membership_created_date
        ,subscription_cancelled_at_utc as most_recent_membership_cancelled_date
    from memberships
    qualify row_number() over(partition by user_id order by subscription_created_at_utc desc) = 1
)

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
        ,identity.first_name
        ,identity.last_name
        ,phone_number.phone_type
        ,phone_number.phone_number
        ,phone_number.does_allow_sms
        ,user_contacts.last_call_at_utc
        ,user_contacts.total_calls
        ,user_contacts.call_result
        ,most_recent_membership.most_recent_membership_created_date
        ,most_recent_membership.most_recent_membership_cancelled_date
        ,membership_count.user_id is not null and total_completed_membership_orders > 0 as is_member
        ,membership_count.user_id is not null and membership_count.total_uncancelled_memberships = 0 as is_cancelled_member
        ,user_order_activity.order_user_id is null as is_lead
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_ala_carte_order_count > 0 and zeroifnull(membership_count.total_membership_count) = 0 as is_purchasing_customer
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_membership_order_count > 0 as is_purchasing_member
        ,user_order_activity.user_id is not null and user_order_activity.last_90_days_paid_membership_order_count > 0 as is_active_member_90_day
        
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
        ,zeroifnull(user_order_activity.lifetime_net_revenue) as lifetime_net_revenue
        ,zeroifnull(user_order_activity.lifetime_paid_order_count) as lifetime_paid_order_count
        ,zeroifnull(user_order_activity.total_completed_unpaid_uncancelled_orders) as total_completed_unpaid_uncancelled_orders
        ,zeroifnull(user_order_activity.total_paid_ala_carte_order_count) as total_paid_ala_carte_order_count
        ,zeroifnull(user_order_activity.total_paid_membership_order_count) as total_paid_membership_order_count
        ,zeroifnull(user_order_activity.last_90_days_paid_membership_order_count) as last_90_days_paid_membership_order_count
        ,zeroifnull(user_order_activity.total_paid_gift_order_count) as total_paid_gift_order_count
        ,zeroifnull(user_order_activity.six_month_net_revenue) as six_month_net_revenue
        ,zeroifnull(user_order_activity.twelve_month_net_revenue) as twelve_month_net_revenue
        ,zeroifnull(user_order_activity.six_month_paid_order_count) as six_month_paid_order_count
        ,zeroifnull(user_order_activity.twelve_month_purchase_count) as twelve_month_purchase_count
        ,zeroifnull(user_order_activity.last_90_days_paid_order_count) as last_90_days_paid_order_count
        ,zeroifnull(user_order_activity.last_180_days_paid_order_count) as last_180_days_paid_order_count
        ,zeroifnull(user_order_activity.recent_delivered_order_count) as recent_delivered_order_count
        ,zeroifnull(user_order_activity.six_month_net_revenue_percentile) as six_month_net_revenue_percentile
        ,zeroifnull(user_order_activity.twelve_month_net_revenue_percentile) as twelve_month_net_revenue_percentile
        ,zeroifnull(user_order_activity.lifetime_net_revenue_percentile) as lifetime_net_revenue_percentile
        ,zeroifnull(user_order_activity.total_california_orders) as total_california_orders

    from users
        left join membership_count on users.user_id = membership_count.user_id
        left join user_order_activity on users.user_id = user_order_activity.user_id
        left join phone_number on users.phone_number_id = phone_number.phone_number_id
        left join ccpa_users on users.user_token = ccpa_users.user_token
        left join user_visits on users.user_id = user_visits.user_id
        left join postal_code on users.user_zip = postal_code.postal_code
        left join identity on users.user_id = identity.user_id
        left join user_contacts on users.user_token = user_contacts.user_token
        left join most_recent_membership on users.user_id = most_recent_membership.user_id 
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
        
        ,first_name
        ,last_name
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
        ,lifetime_net_revenue
        ,lifetime_paid_order_count
        ,total_completed_unpaid_uncancelled_orders
        ,total_paid_ala_carte_order_count
        ,total_paid_membership_order_count
        ,last_90_days_paid_membership_order_count
        ,last_90_days_paid_order_count
        ,last_180_days_paid_order_count
        ,six_month_paid_order_count
        ,twelve_month_purchase_count
        ,total_paid_gift_order_count
        ,recent_delivered_order_count
        ,six_month_net_revenue
        ,twelve_month_net_revenue
        ,six_month_net_revenue_percentile
        ,twelve_month_net_revenue_percentile
        ,lifetime_net_revenue_percentile
        ,total_california_orders
        ,total_calls as total_phone_burner_calls
        ,call_result as last_phone_burner_call_result
        ,attributed_visit_id
        
        ,nullif(
            greatest(
                nvl(unsubscribed_all_at_utc,'1970-01-01')
                ,nvl(email_subscribed_often_at_utc,'1970-01-01')
                ,nvl(email_subscribed_weekly_at_utc,'1970-01-01')
            )
        ,'1970-01-01') as last_email_preference_date
        
        ,case
            when greatest(
                    nvl(unsubscribed_all_at_utc,'1970-01-01')
                    ,nvl(email_subscribed_often_at_utc,'1970-01-01')
                    ,nvl(email_subscribed_weekly_at_utc,'1970-01-01')
                ) = unsubscribed_all_at_utc 
            then 'UNSUBSCRIBED ALL'
            when greatest(
                    nvl(unsubscribed_all_at_utc,'1970-01-01')
                    ,nvl(email_subscribed_often_at_utc,'1970-01-01')
                    ,nvl(email_subscribed_weekly_at_utc,'1970-01-01')
                ) = email_subscribed_often_at_utc 
                or (email_subscribed_often_at_utc is null 
                    and unsubscribed_all_at_utc is null 
                    and email_subscribed_often_at_utc is null 
                    and has_opted_in_to_emails
                )
            then 'OFTEN'
            when greatest(
                    nvl(unsubscribed_all_at_utc,'1970-01-01')
                    ,nvl(email_subscribed_often_at_utc,'1970-01-01')
                    ,nvl(email_subscribed_weekly_at_utc,'1970-01-01')
                ) = email_subscribed_weekly_at_utc 
            then 'WEEKLY'
            else 'NO SETTING'
        end as user_email_preference
        
        ,is_member
        ,is_cancelled_member
        ,is_lead
        ,is_purchasing_customer
        ,is_purchasing_member
        ,is_active_member_90_day
        ,is_banned
        ,does_allow_sms
        ,has_opted_in_to_emails
        ,last_call_at_utc is not null as has_phone_burner_contact
        ,most_recent_membership_created_date >= last_call_at_utc as did_create_membership_after_call
        ,last_sign_in_at_utc
        ,last_call_at_utc
        ,most_recent_membership_created_date
        ,most_recent_membership_cancelled_date
        ,created_at_utc
        ,updated_at_utc
    from user_joins
    where not is_ccpa
)

select * from final

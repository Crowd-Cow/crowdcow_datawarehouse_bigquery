with

users as (select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null)
,memberships as (select * from {{ ref('stg_cc__subscriptions') }})
,orders as (select * from {{ ref('stg_cc__orders') }})

,membership_count as (
    select
        user_id
        ,count(subscription_id) as total_membership_count
        ,count_if(subscription_cancelled_at_utc is not null) as total_cancelled_membership_count
        ,count(subscription_id) - count_if(subscription_cancelled_at_utc is not null) as total_uncancelled_memberships
    from memberships
    group by 1
)

,order_count as (
    select
        user_id
        ,count(order_id) as total_order_count
        ,count_if(order_cancelled_at_utc is null and order_paid_at_utc is not null and subscription_id is null) as total_paid_ala_carte_order_count
        ,count_if(order_cancelled_at_utc is null and order_paid_at_utc is not null and subscription_id is not null) total_paid_membership_order_count
        ,count_if(order_cancelled_at_utc is null and order_paid_at_utc is not null and subscription_id is not null and order_paid_at_utc::date - sysdate()::date <= 90) as total_active_order_count
    from orders
    group by 1
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
from users
    left join membership_count on users.user_id = membership_count.user_id
    left join order_count on users.user_id = order_count.user_id
)
select
    user_id
    ,phone_number_id
    ,user_type
    ,user_gender
    ,user_email_name
    ,user_email
    ,user_roles_for_access
    ,user_zip
    ,user_token
    ,user_referrer_token
    ,user_cow_cash_balance_usd
    ,user_support_status
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
with

user as ( select * from {{ ref('users') }} )
,vip as ( select distinct user_id from {{ ref('user_tags') }} where tag_key like 'vip%')

,user_list as (
    select distinct
        user.user_email
        ,user.phone_number
        ,user.first_name
        ,user.last_name
        ,'us' as country
        ,user.state_code as user_state
        ,user.user_zip
        ,user.is_lead as is_customer_lead
        ,true as is_lead_and_registered_user
        ,user.lifetime_paid_order_count > 0 as is_customer
        ,user.last_90_days_paid_order_count > 0 as is_standard_customer
        ,user.last_90_days_paid_order_count > 0 and user.is_purchasing_customer as is_active_non_member_90_day
        ,user.is_active_member_90_day and not is_cancelled_member as is_active_member_90_day
        ,user.recent_purchaser as is_recent_customer
        ,user.lapsed_purchaser as is_lapsed_customer
        ,user.dormant_purchaser as is_dormant_customer
        ,user.lifetime_paid_order_count > 0 and user.twelve_month_purchase_count = 0 as is_inactive_customer
        ,user.twelve_month_purchase_count >= 4 as is_frequent_customer
        ,user.lifetime_net_revenue > 0 and user.lifetime_net_revenue_percentile > 80 as is_top_spender_customer
        ,vip.user_id is not null as is_vip_customer
        ,user.twelve_month_purchase_count > 0 as has_orders_last_12_months
    from user
        left join vip on user.user_id = vip.user_id
    where user_type in ('CUSTOMER','EMPLOYEE')
)

,prep_user_fields as (
    select
        trim(lower(replace(user_email,'>',''))) as email
        ,trim(lower(first_name)) as first_name
        ,trim(lower(last_name)) as last_name
        ,country
        ,trim(lower(user_state)) as state
        ,trim(user_zip) as zip
        ,trim(phone_number) as phone_number
        ,is_customer_lead
        ,is_lead_and_registered_user
        ,is_customer
        ,is_standard_customer
        ,is_vip_customer
        ,is_active_non_member_90_day
        ,is_active_member_90_day
        ,is_recent_customer
        ,is_lapsed_customer
        ,is_dormant_customer
        ,is_inactive_customer
        ,is_frequent_customer
        ,is_top_spender_customer
        ,has_orders_last_12_months
    from user_list
)

,validate_email_phone as (
    select
        IF(REGEXP_CONTAINS(email, r'^[a-z0-9!._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'), email, NULL) AS email
        ,first_name
        ,last_name
        ,country
        ,state
        ,zip
        ,IF(REGEXP_CONTAINS(phone_number, r'^\\([0-9]{3}\\) [0-9]{3}-[0-9]{4}$'), CONCAT('1 ', phone_number), NULL) AS phone_number
        ,is_customer_lead
        ,is_lead_and_registered_user
        ,is_customer
        ,is_standard_customer
        ,is_vip_customer
        ,is_active_non_member_90_day
        ,is_active_member_90_day
        ,is_recent_customer
        ,is_lapsed_customer
        ,is_dormant_customer
        ,is_inactive_customer
        ,is_frequent_customer
        ,is_top_spender_customer
    from prep_user_fields
)

select distinct
    *
from validate_email_phone 
where email is not null

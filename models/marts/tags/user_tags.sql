{{
    config(
        post_hook = [
            "copy into @USER_SEGMENTATION_TAGS/users/user_tags.csv from (
                    select 
                        row_number() over(order by created_at_utc,user_id) as id
                        ,tag_key as key,user_id,tag_purpose as purpose
                        ,created_at_utc as created_at
                        ,updated_at_utc as updated_at 
                    from {{ this }}
                )
                single = true
                overwrite = true
                header = true
                max_file_size = 4900000000
                file_format='csv_with_headers';"
        ]
    )
}}

with

employee as (
    {{ generate_tag('users','user_id','employee','user_segment') }}
    where user_type = 'EMPLOYEE'
)

,recent_delivery as (
    {{ generate_tag('users','user_id','recent_delivery','user_segment')}}
    where user_type in ('CUSTOMER','EMPLOYEE') and recent_delivered_order_count >= 1
)

,vip_new_customer as (
    {{ generate_tag('users','user_id','vip_new_customer','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and total_completed_unpaid_uncancelled_orders = 1 and lifetime_paid_order_count = 0
)

,vip_superstar as (
    {{ generate_tag('users','user_id','vip_superstar','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and six_month_net_revenue > 0 and six_month_net_revenue_percentile > 98
)

,vip_frequent as (
    {{ generate_tag('users','user_id','vip_frequent','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and twelve_month_purchase_count >= 4
)

,vip_profit as (
    {{ generate_tag('users','user_id','vip_profit','user_segment') }}
    where user_type = 'CUSTOMER' and six_month_net_revenue > 0 and six_month_net_revenue_percentile > 80
)

,member as (
    {{ generate_tag('users','user_id','has_ever_been_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_member
)

,non_member as (
    {{ generate_tag('users','user_id','non_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and not is_member
)

,internal as (
    {{ generate_tag('users','user_id','internal','user_segment') }}
    where user_type = 'INTERNAL'
)

,lead as (
    {{ generate_tag('users','user_id','lead','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_lead
)

,cancelled_member as (
    {{ generate_tag('users','user_id','cancelled_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_member and is_cancelled_member
)

,uncancelled_member as (
    {{ generate_tag('users','user_id','uncancelled_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_member and not is_cancelled_member
)

,purchasing_customer as (
    {{ generate_tag('users','user_id','purchasing_customer','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_purchasing_customer
)

,purchasing_member as (
    {{ generate_tag('users','user_id','purchasing_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_purchasing_member
)

,active_member_90_day as (
    {{ generate_tag('users','user_id','90_day_active_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_member and not is_cancelled_member and last_90_days_paid_order_count > 0
)

,active_member_180_day as (
    {{ generate_tag('users','user_id','180_day_active_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_member and not is_cancelled_member and last_180_days_paid_order_count > 0
)

,inactive_member_90_day as (
    {{ generate_tag('users','user_id','90_day_inactive_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_member and not is_cancelled_member and last_90_days_paid_order_count = 0
)

,inactive_member_180_day as (
    {{ generate_tag('users','user_id','180_day_inactive_member','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and is_member and not is_cancelled_member and last_180_days_paid_order_count = 0
)

,gift_giver as (
    {{ generate_tag('users','user_id','gift_giver','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and total_paid_gift_order_count > 0
)

,california_customer as (
    {{ generate_tag('users','user_id','california_customer','user_segment') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and total_california_orders > 0
)

,jp_wagyu_bronze as (
    {{ generate_tag('users','user_id','jp_wagyu_bronze','user_segment','japanese_buyers_club_revenue') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and japanese_buyers_club_revenue between 200 and 499.99
)

,jp_wagyu_silver as (
    {{ generate_tag('users','user_id','jp_wagyu_silver','user_segment','japanese_buyers_club_revenue') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and japanese_buyers_club_revenue between 500 and 999.99
)

,jp_wagyu_gold as (
    {{ generate_tag('users','user_id','jp_wagyu_gold','user_segment','japanese_buyers_club_revenue') }}
    where user_type in ('CUSTOMER','EMPLOYEE') and japanese_buyers_club_revenue >= 1000
)

select * from employee
union all
select * from recent_delivery
union all
select * from vip_new_customer
union all
select * from vip_superstar
union all
select * from vip_frequent
union all
select * from vip_profit
union all
select * from member
union all
select * from non_member
union all
select * from internal
union all
select * from lead
union all
select * from cancelled_member
union all
select * from uncancelled_member
union all
select * from purchasing_customer
union all
select * from purchasing_member
union all
select * from active_member_90_day
union all
select * from active_member_180_day
union all
select * from inactive_member_90_day
union all
select * from inactive_member_180_day
union all
select * from gift_giver
union all
select * from california_customer
union all
select * from jp_wagyu_bronze
union all
select * from jp_wagyu_silver
union all
select * from jp_wagyu_gold

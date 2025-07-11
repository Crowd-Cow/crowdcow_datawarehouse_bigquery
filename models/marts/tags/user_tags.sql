with

employee as (
    {{ generate_tag('users','user_id','employee','user_segment', 'null') }}
    where user_type = 'EMPLOYEE'
)

,recent_delivery as (
    {{ generate_tag('users','user_id','recent_delivery','user_segment', 'null')}}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and recent_delivered_order_count >= 1
)

,vip_new_customer as (
    {{ generate_tag('users','user_id','vip_new_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and total_completed_unpaid_uncancelled_orders = 1 and lifetime_paid_order_count = 0
)

,vip_superstar as (
    {{ generate_tag('users','user_id','vip_superstar','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and six_month_net_revenue > 0 and six_month_net_revenue_percentile > 98
)

,vip_frequent as (
    {{ generate_tag('users','user_id','vip_frequent','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and twelve_month_purchase_count >= 4
)
,vip_top10_orderhistory as (
    {{ generate_tag('users','user_id','vip_top10_orderhistory','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and lifetime_paid_order_count >= 4 and lifetime_paid_order_count_percentile > 90
)
,vip_profit as (
    {{ generate_tag('users','user_id','vip_profit','user_segment', 'null') }}
    where user_type = 'CUSTOMER' and six_month_net_revenue > 0 and six_month_net_revenue_percentile > 80
)
,vip_top20_spendhistory as (
    {{ generate_tag('users','user_id','vip_top20_spendhistory','user_segment', 'null') }}
    where user_type = 'CUSTOMER' and lifetime_net_revenue > 0 and lifetime_net_revenue_percentile > 80
)
,super_vip_spendhistory as (
    {{ generate_tag('users','user_id','super_vip_spendhistory','user_segment', 'null') }}
    where user_type = 'CUSTOMER' and lifetime_net_revenue > 0 and lifetime_net_revenue_percentile > 98
)

,vip_priority as (
    {{ generate_tag('users','user_id','vip_priority','user_segment', 'null') }}
    where user_type = 'CUSTOMER' and six_month_paid_order_count > 0 and six_month_gross_profit_percentile >= 80
)

,member as (
    {{ generate_tag('users','user_id','has_ever_been_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_member
)

,non_member as (
    {{ generate_tag('users','user_id','non_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and not is_member
)

,internal as (
    {{ generate_tag('users','user_id','internal','user_segment', 'null') }}
    where user_type = 'INTERNAL'
)

,lead as (
    {{ generate_tag('users','user_id','lead','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_lead
)

,cancelled_member as (
    {{ generate_tag('users','user_id','cancelled_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_member and is_cancelled_member
)

,uncancelled_member as (
    {{ generate_tag('users','user_id','uncancelled_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_member and not is_cancelled_member
)

,purchasing_customer as (
    {{ generate_tag('users','user_id','purchasing_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_purchasing_customer
)

,purchasing_member as (
    {{ generate_tag('users','user_id','purchasing_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_purchasing_member
)

,active_customer_90_day as (
    {{ generate_tag('users','user_id','90_day_active_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and not is_active_member_90_day and last_90_days_paid_order_count > 0
)

,active_customer_180_day as (
    {{ generate_tag('users','user_id','180_day_active_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and not is_active_member_90_day and last_180_days_paid_order_count > 0
)

,active_member_90_day as (
    {{ generate_tag('users','user_id','90_day_active_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_member and not is_cancelled_member and last_90_days_paid_order_count > 0
)

,active_member_180_day as (
    {{ generate_tag('users','user_id','180_day_active_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_member and not is_cancelled_member and last_180_days_paid_order_count > 0
)

,inactive_member_90_day as (
    {{ generate_tag('users','user_id','90_day_inactive_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_member and not is_cancelled_member and last_90_days_paid_order_count = 0
)

,twelve_month_japanese_wagyu_revenue as (
    {{ generate_tag('users','user_id','japanese_wagyu_interest','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and twelve_month_japanese_wagyu_revenue > 0
)

,inactive_member_180_day as (
    {{ generate_tag('users','user_id','180_day_inactive_member','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and is_member and not is_cancelled_member and last_180_days_paid_order_count = 0
)

,gift_giver as (
    {{ generate_tag('users','user_id','gift_giver','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and total_paid_gift_order_count > 0
)

,california_customer as (
    {{ generate_tag('users','user_id','california_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and total_california_orders > 0
)

,churned_customer as (
    {{ generate_tag('users','user_id','churned_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lifetime_paid_order_count > 0 and last_90_days_paid_order_count = 0
)

,all_leads as (
    {{ generate_tag('users','user_id','all_leads','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and all_leads
)
,hot_lead as (
    {{ generate_tag('users','user_id','hot_lead','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and hot_lead
)
,warm_lead as (
    {{ generate_tag('users','user_id','warm_lead','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and warm_lead
)
,cold_lead as (
    {{ generate_tag('users','user_id','cold_lead','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and cold_lead
)
,purchaser as (
    {{ generate_tag('users','user_id','purchaser','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and purchaser
)
,recent_purchaser as (
    {{ generate_tag('users','user_id','recent_purchaser','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and recent_purchaser
)
,lapsed_purchaser as (
    {{ generate_tag('users','user_id','lapsed_purchaser','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lapsed_purchaser
)
,dormant_purchaser as (
    {{ generate_tag('users','user_id','dormant_purchaser','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and dormant_purchaser
)
,alc_customer as (
    {{ generate_tag('users','user_id','alc_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and alc_customer
)
,new_alc as (
    {{ generate_tag('users','user_id','new_alc','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and new_alc
)
,new_alc_wagyu as (
    {{ generate_tag('users','user_id','new_alc_wagyu','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and new_alc_wagyu
)
,recent_alc as (
    {{ generate_tag('users','user_id','recent_alc','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and recent_alc
)
,lapsed_alc as (
    {{ generate_tag('users','user_id','lapsed_alc','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lapsed_alc
)
,dormant_alc as (
    {{ generate_tag('users','user_id','dormant_alc','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and dormant_alc
)
,new_subscriber as (
    {{ generate_tag('users','user_id','new_subscriber','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and new_subscriber
)
,new_subscriber_4_weeks as (
    {{ generate_tag('users','user_id','new_subscriber_4_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and new_subscriber_4_weeks
)
,new_subscriber_5_8_weeks as (
    {{ generate_tag('users','user_id','new_subscriber_5_8_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and new_subscriber_5_8_weeks
)
,new_subscriber_12_weeks as (
    {{ generate_tag('users','user_id','new_subscriber_12_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and new_subscriber_12_weeks
)
,active_subscriber_4_weeks as (
    {{ generate_tag('users','user_id','active_subscriber_4_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and active_subscriber_4_weeks
)
,active_subscriber_5_8_weeks as (
    {{ generate_tag('users','user_id','active_subscriber_5_8_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and active_subscriber_5_8_weeks
)
,active_subscriber_12_weeks as (
    {{ generate_tag('users','user_id','active_subscriber_12_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and active_subscriber_12_weeks
)
,lapsed_subscriber_4_weeks as (
    {{ generate_tag('users','user_id','lapsed_subscriber_4_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lapsed_subscriber_4_weeks
)
,lapsed_subscriber_5_8_weeks as (
    {{ generate_tag('users','user_id','lapsed_subscriber_5_8_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lapsed_subscriber_5_8_weeks
)
,lapsed_subscriber_12_weeks as (
    {{ generate_tag('users','user_id','lapsed_subscriber_12_weeks','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lapsed_subscriber_12_weeks
)
,active_cancelled_subscriber as (
    {{ generate_tag('users','user_id','active_cancelled_subscriber','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and active_cancelled_subscriber
)
,recent_cancelled_subscriber as (
    {{ generate_tag('users','user_id','recent_cancelled_subscriber','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and recent_cancelled_subscriber
)
,lapsed_cancelled_subscriber as (
    {{ generate_tag('users','user_id','lapsed_cancelled_subscriber','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lapsed_cancelled_subscriber
)
,gifts_sent as (
    {{ generate_tag('users','user_id','number_of_gifts_sent','user_data_point', 'total_paid_gift_order_count') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and total_paid_gift_order_count > 0
)
,last_bison_order_date as (
    {{ generate_tag('users','user_id','last_bisons_order_date','user_data_point','most_recent_bison_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_bison_order_date is not null
) 

,most_recent_beef_order_date as (
    {{ generate_tag('users','user_id','last_beef_order_date','user_data_point', 'most_recent_beef_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and most_recent_beef_order_date is not null
)

,most_recent_chicken_order_date as (
    {{ generate_tag('users','user_id','last_chicken_order_date','user_data_point','most_recent_chicken_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_chicken_order_date is not null
)
,most_recent_japanse_wagyu_order_date as (
    {{ generate_tag('users','user_id','last_japanse_wagyu_order_date','user_data_point','most_recent_japanse_wagyu_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_japanse_wagyu_order_date is not null
)
,most_recent_lamb_order_date as (
    {{ generate_tag('users','user_id','last_lamb_order_date','user_data_point','most_recent_lamb_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_lamb_order_date is not null
)
,most_recent_pork_order_date as (
    {{ generate_tag('users','user_id','last_pork_order_date','user_data_point','most_recent_pork_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_pork_order_date is not null
)
,most_recent_seafood_order_date as (
    {{ generate_tag('users','user_id','last_seafood_order_date','user_data_point','most_recent_seafood_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_seafood_order_date is not null
)
,most_recent_sides_order_date as (
    {{ generate_tag('users','user_id','last_sides_order_date','user_data_point','most_recent_sides_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_sides_order_date is not null
)
,most_recent_turkey_order_date as (
    {{ generate_tag('users','user_id','last_turkey_order_date','user_data_point','most_recent_turkey_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_turkey_order_date is not null
)
,most_recent_wagyu_order_date as (
    {{ generate_tag('users','user_id','last_wagyu_order_date','user_data_point','most_recent_wagyu_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_wagyu_order_date is not null
)
,most_recent_bundle_order_date as (
    {{ generate_tag('users','user_id','last_bundle_order_date','user_data_point','most_recent_bundle_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and most_recent_bundle_order_date is not null
)
,has_ordered_beef as (
    {{ generate_tag('users','user_id','has_ordered_beef','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and beef_revenue > 0
)
,has_ordered_bison as (
    {{ generate_tag('users','user_id','has_ordered_bison','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and bison_revenue > 0
)
,has_ordered_chicken as (
    {{ generate_tag('users','user_id','has_ordered_chicken','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and chicken_revenue > 0
)
,has_ordered_japanese_wagyu as (
    {{ generate_tag('users','user_id','has_ordered_japanese_wagyu','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and japanese_wagyu_revenue > 0
)
,has_ordered_lamb as (
    {{ generate_tag('users','user_id','has_ordered_lamb','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and lamb_revenue > 0
)
,has_ordered_pork as (
    {{ generate_tag('users','user_id','has_ordered_pork','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and pork_revenue > 0
)
,has_ordered_sides as (
    {{ generate_tag('users','user_id','has_ordered_sides','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and sides_revenue > 0
)
,has_ordered_turkey as (
    {{ generate_tag('users','user_id','has_ordered_turkey','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and turkey_revenue > 0
)
,has_ordered_wagyu as (
    {{ generate_tag('users','user_id','has_ordered_wagyu','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and wagyu_revenue > 0
)
,has_ordered_bundle as (
    {{ generate_tag('users','user_id','has_ordered_bundle','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and bundle_revenue > 0
)
,has_ordered_seafood as (
    {{ generate_tag('users','user_id','has_ordered_seafood','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and seafood_revenue > 0
)

,last_30_days_paid_order_count as (
    {{ generate_tag('users','user_id','last_30_paid_order_count','user_data_point','last_30_days_paid_order_count') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_30_days_paid_order_count > 0
)
,last_60_days_paid_order_count as (
    {{ generate_tag('users','user_id','last_60_paid_order_count','user_data_point','last_60_days_paid_order_count') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_60_days_paid_order_count > 0
)
,last_90_days_paid_order_count as (
    {{ generate_tag('users','user_id','last_90_paid_order_count','user_data_point','last_90_days_paid_order_count') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_90_days_paid_order_count > 0
)
,last_120_days_paid_order_count as (
    {{ generate_tag('users','user_id','last_120_paid_order_count','user_data_point','last_120_days_paid_order_count') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_120_days_paid_order_count > 0
)
,last_180_days_paid_order_count as (
    {{ generate_tag('users','user_id','last_180_paid_order_count','user_data_point','last_180_days_paid_order_count') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_180_days_paid_order_count > 0
)
,lifetime_paid_order_count as (
    {{ generate_tag('users','user_id','lifetime_paid_order_count','user_data_point','lifetime_paid_order_count') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and lifetime_paid_order_count > 0
)
,lifetime_net_revenue as (
    {{ generate_tag('users','user_id','lifetime_paid_net_revenue','user_data_point','lifetime_net_revenue') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and lifetime_net_revenue > 0
)
,user_average_order_value as (
    {{ generate_tag('users','user_id','average_paid_order_value','user_data_point','user_average_order_value') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and user_average_order_value > 0
)
,last_paid_order_date as (
    {{ generate_tag('users','user_id','last_paid_order_date','user_data_point','last_paid_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_paid_order_date is not null
)
,last_paid_order_value as (
    {{ generate_tag('users','user_id','last_paid_order_value','user_data_point','last_paid_order_value') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_paid_order_value > 0
)
,last_paid_moolah_order_date as (
    {{ generate_tag('users','user_id','last_paid_moolah_order_date','user_data_point','last_paid_moolah_order_date') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_paid_moolah_order_date is not null
)
,has_redeemed_moolah as (
    {{ generate_tag('users','user_id','has_redeemed_moolah','user_data_point','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and redeemed_moolah_points < 0
)
,last_14_days_impacful_customer_reschedules as (
    {{ generate_tag('users','user_id','last_14_days_impacful_customer_reschedules','user_data_point','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and last_14_days_impacful_customer_reschedules > 0
)
,referrals_sent as (
    {{ generate_tag('users','user_id','referrals_sent','user_data_point','referrals_sent') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and referrals_sent > 0
)
,referrals_redeemed as (
    {{ generate_tag('users','user_id','referrals_redeemed','user_data_point','referrals_redeemed') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and referrals_redeemed > 0
)
,has_redeemed_gift_card as (
    {{ generate_tag('users','user_id','has_redeemed_gift_card','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and has_redeemed_gift_card 
)

,vip_important_relationship as (
    {{ generate_tag('users','user_id','vip_important_relationship','user_segment','null') }}
    where user_type in ('CUSTOMER','EMPLOYEE','INTERNAL') and has_redeemed_gc_vip_referral 
)

,new_customer as (
    {{ generate_tag('users','user_id','new_customer','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lifetime_paid_order_count > 0 and lifetime_paid_order_count <= 3 
)

,new_meat_collector as (
    {{ generate_tag('users','user_id','new_meat_collector','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lifetime_paid_order_count > 0 and lifetime_paid_order_count <= 3  and user_average_order_value >= 400 and average_spend_per_lb >= 50 and twelve_month_purchase_count > 0
)
,existing_meat_collector as (
    {{ generate_tag('users','user_id','existing_meat_collector','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lifetime_paid_order_count >= 4  and user_average_order_value >= 400 and average_spend_per_lb >= 50 and twelve_month_purchase_count > 0
)
,inactive_meat_collector as (
    {{ generate_tag('users','user_id','inactive_meat_collector','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lifetime_paid_order_count >= 4  and user_average_order_value >= 400 and average_spend_per_lb >= 50 and twelve_month_purchase_count = 0
)
,inactive_new_meat_collector as (
    {{ generate_tag('users','user_id','inactive_new_meat_collector','user_segment', 'null') }}
    where user_type in ('CUSTOMER','EMPLOYEE', 'INTERNAL') and lifetime_paid_order_count > 0 and lifetime_paid_order_count <= 3  and user_average_order_value >= 400 and average_spend_per_lb >= 50 and twelve_month_purchase_count = 0
)


,union_tags as (
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
select * from vip_priority
union all
select * from vip_top20_spendhistory
union all 
select * from vip_top10_orderhistory
union all
select * from super_vip_spendhistory
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
select * from churned_customer
union all
select * from active_customer_180_day
union all 
select * from active_customer_90_day
union all 
select * from twelve_month_japanese_wagyu_revenue
union all 
select * from all_leads
union all 
select * from hot_lead
union all 
select * from warm_lead
union all 
select * from cold_lead
union all 
select * from purchaser
union all 
select * from recent_purchaser
union all 
select * from lapsed_purchaser
union all 
select * from dormant_purchaser
union all 
select * from alc_customer
union all 
select * from new_alc 
union all 
select * from new_alc_wagyu
union all 
select * from recent_alc
union all 
select * from lapsed_alc
union all 
select * from dormant_alc
union all 
select * from new_subscriber
union all 
select * from new_subscriber_4_weeks
union all 
select * from new_subscriber_5_8_weeks
union all 
select * from new_subscriber_12_weeks
union all 
select * from active_subscriber_4_weeks
union all 
select * from active_subscriber_5_8_weeks
union all 
select * from active_subscriber_12_weeks
union all 
select * from lapsed_subscriber_4_weeks
union all 
select * from lapsed_subscriber_5_8_weeks
union all 
select * from lapsed_subscriber_12_weeks
union all 
select * from active_cancelled_subscriber 
union all 
select * from recent_cancelled_subscriber 
union all 
select * from lapsed_cancelled_subscriber 
union all 
select * from gifts_sent
union all 
select * from most_recent_beef_order_date  
union all 
select * from last_bison_order_date  
union all 
select * from most_recent_chicken_order_date
union all 
select * from most_recent_japanse_wagyu_order_date
union all 
select * from most_recent_lamb_order_date
union all 
select * from most_recent_pork_order_date
union all 
select * from most_recent_seafood_order_date
union all 
select * from most_recent_sides_order_date
union all 
select * from most_recent_turkey_order_date
union all 
select * from most_recent_wagyu_order_date
union all 
select * from most_recent_bundle_order_date

union all
select * from has_ordered_beef
union all
select * from has_ordered_bison
union all
select * from has_ordered_chicken
union all
select * from has_ordered_japanese_wagyu
union all
select * from has_ordered_lamb
union all
select * from has_ordered_pork
union all
select * from has_ordered_sides
union all
select * from has_ordered_turkey
union all
select * from has_ordered_wagyu
union all
select * from has_ordered_bundle
union all
select * from has_ordered_seafood

union all
select * from last_30_days_paid_order_count
union all
select * from last_60_days_paid_order_count
union all
select * from last_90_days_paid_order_count
union all
select * from last_120_days_paid_order_count
union all
select * from last_180_days_paid_order_count



union all 
select * from lifetime_paid_order_count
union all 
select * from lifetime_net_revenue
union all 
select * from user_average_order_value
union all 
select * from last_paid_order_date
union all 
select * from last_paid_order_value
union all 
select * from has_redeemed_moolah
union all 
select * from last_paid_moolah_order_date
union all 
select * from last_14_days_impacful_customer_reschedules
union all 
select * from referrals_sent
union all 
select * from referrals_redeemed
union all 
select * from has_redeemed_gift_card
union all
select * from new_customer
union all
select * from vip_important_relationship

union all 
select * from new_meat_collector
union all 
select * from existing_meat_collector
union all 
select * from inactive_meat_collector
union all 
select * from inactive_new_meat_collector

)

select 
row_number() over(order by created_at_utc,user_id) as id,
*
from union_tags




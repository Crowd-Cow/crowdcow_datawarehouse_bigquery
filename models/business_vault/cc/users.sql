
with

users as (select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null)
,user_order_activity as ( select * from {{ ref('int_user_order_activity') }} )
,user_membership as ( select * from {{ ref('int_user_memberships') }} )
,phone_number as ( select * from {{ ref('stg_cc__phone_numbers') }} )
,visit as ( select user_id, visit_id, started_at_utc from {{ ref('visit_classification') }} order by started_at_utc asc )
,postal_code as ( select * from {{ ref('stg_cc__postal_codes') }} )
,identity as ( select * from {{ ref('int_recent_user_identities') }} )
,contact as ( select * from {{ ref('contacts') }} )
,referrals as ( select * from {{ ref('referrals') }})
,gift_card_transaction_history as ( select * from {{ ref('gift_card_transaction_history') }} )
,ccpa as (select * from {{ ref('stg_gs__ccpa_requests') }}) 
,fb_split as (select * from {{ ref('stg_reference__fb_split') }} where user_token is not null)

,ccpa_users as (
    select 
        DISTINCT
        CASE
            WHEN REGEXP_CONTAINS(ccpa.admin_link, r'CROWDCOW.COM/ADMIN/') THEN LOWER(SPLIT(ccpa.admin_link, '/')[OFFSET(4)])
            WHEN ccpa.email IS NOT NULL THEN users.user_token
            ELSE NULL
        END AS user_token
    from ccpa
        left join users on ccpa.email = users.user_email
)

,user_contacts as (
    select 
        user_token
        ,owner_name
        ,last_call_at_utc
        ,total_calls
        ,call_result
    from contact
    where user_token is not null
    qualify row_number() over(partition by user_token order by date_modified_at_utc desc) = 1
)

,user_visits as (
    select
        user_id
        ,first_value(visit_id) over(partition by user_id order by started_at_utc, visit_id) as first_visit_id
    from visit
    where user_id is not null
    qualify row_number() over(partition by user_id order by started_at_utc, visit_id) = 1
)

,user_referrals as (
    select 
        referrer_user_id as user_id 
        ,count(distinct referee_user_id) as referrals_sent
        ,count( distinct if(purchased_at_utc is not null, referee_user_id,null)) as referrals_redeemed
    from referrals 
    group by 1
)
,user_gift_card_transaction_history as (
    select 
        distinct
        redemption_user_id
    from gift_card_transaction_history
    where redemption_user_id is not null 
)

,user_gift_card_redemption_vip_affiliate as (
    select 
        distinct
        redemption_user_id
    from gift_card_transaction_history
    where redemption_user_id is not null 
    and batch_uuid = 'PBIBOPVNEN'
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
        ,user_contacts.owner_name
        ,user_membership.first_promotion_type
        ,user_membership.current_promotion_type
        ,user_membership.current_renew_period
        ,user_membership.total_membership_count
        ,user_membership.total_uncancelled_memberships
        ,user_membership.membership_tenure_months
        ,user_membership.first_membership_created_date
        ,user_membership.most_recent_membership_created_date
        ,user_membership.most_recent_membership_cancelled_date
        ,user_membership.user_id is not null and user_order_activity.total_completed_membership_orders > 0 as is_member
        ,user_membership.user_id is not null and user_membership.total_uncancelled_memberships = 0 as is_cancelled_member
        ,ifnull(user_membership.is_current_promotion_ffl,False) as is_current_promotion_ffl
        ,ifnull(user_membership.is_first_promotion_ffl,False) as is_first_promotion_ffl
        ,user_order_activity.order_user_id is null as is_lead
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_ala_carte_order_count > 0 and coalesce(user_membership.total_membership_count,0) = 0 as is_purchasing_customer
        ,user_order_activity.user_id is not null and user_order_activity.total_paid_membership_order_count > 0 as is_purchasing_member
        ,user_order_activity.user_id is not null and user_order_activity.last_90_days_paid_membership_order_count > 0 and user_membership.total_uncancelled_memberships > 0 as is_active_member_90_day
        ,case
            when user_order_activity.customer_cohort_date < user_order_activity.membership_cohort_date then user_order_activity.membership_cohort_date - user_order_activity.customer_cohort_date
         end as days_from_ala_carte_to_membership

        ,user_order_activity.average_order_frequency_days
        ,user_order_activity.average_membership_order_frequency_days
        ,user_order_activity.average_ala_carte_order_frequency_days
        ,user_order_activity.customer_cohort_date
        ,user_order_activity.membership_cohort_date
        ,user_order_activity.customer_reactivation_date
        ,user_order_activity.first_completed_order_date
        ,user_order_activity.first_completed_order_visit_id
        ,user_order_activity.acquisition_promotion_id
        ,user_order_activity.acquisition_promotion_source
        ,user_order_activity.acquisition_promotion_name
        ,user_visits.first_visit_id
        ,case
            when DATE_DIFF(cast(user_order_activity.first_completed_order_date as date), cast(users.created_at_utc as date), DAY) <= 10 and user_visits.first_visit_id is not null then user_visits.first_visit_id
            else user_order_activity.first_completed_order_visit_id
         end as attributed_visit_id

        ,ccpa_users.user_token is not null as is_ccpa
        ,users.user_banned_at_utc is not null as is_banned
        ,postal_code.state_code
        ,postal_code.city_name
        ,coalesce(user_order_activity.is_rastellis,FALSE) as is_rastellis
        ,coalesce(user_order_activity.is_qvc,FALSE) as is_qvc
        ,coalesce(user_order_activity.is_seabear,FALSE) as is_seabear
        ,coalesce(user_order_activity.is_backyard_butchers,FALSE) as is_backyard_butchers
        ,coalesce(user_order_activity.lifetime_net_revenue) as lifetime_net_revenue
        ,coalesce(user_order_activity.lifetime_net_product_revenue) as lifetime_net_product_revenue
        ,coalesce(user_order_activity.lifetime_paid_order_count) as lifetime_paid_order_count
        ,coalesce(user_order_activity.total_completed_unpaid_uncancelled_orders) as total_completed_unpaid_uncancelled_orders
        ,coalesce(user_order_activity.total_paid_ala_carte_order_count) as total_paid_ala_carte_order_count
        ,coalesce(user_order_activity.total_paid_membership_order_count) as total_paid_membership_order_count
        ,coalesce(user_order_activity.last_90_days_paid_membership_order_count) as last_90_days_paid_membership_order_count
        ,coalesce(user_order_activity.total_paid_gift_order_count) as total_paid_gift_order_count
        ,coalesce(user_order_activity.six_month_net_revenue) as six_month_net_revenue
        ,coalesce(user_order_activity.six_month_gross_profit) as six_month_gross_profit
        ,coalesce(user_order_activity.twelve_month_net_revenue) as twelve_month_net_revenue
        ,coalesce(user_order_activity.six_month_paid_order_count) as six_month_paid_order_count
        ,coalesce(user_order_activity.twelve_month_purchase_count) as twelve_month_purchase_count
        ,coalesce(user_order_activity.last_30_days_paid_order_count) as last_30_days_paid_order_count
        ,coalesce(user_order_activity.last_60_days_paid_order_count) as last_60_days_paid_order_count
        ,coalesce(user_order_activity.last_90_days_paid_order_count) as last_90_days_paid_order_count
        ,coalesce(user_order_activity.last_120_days_paid_order_count) as last_120_days_paid_order_count
        ,coalesce(user_order_activity.last_180_days_paid_order_count) as last_180_days_paid_order_count
        ,coalesce(user_order_activity.recent_delivered_order_count) as recent_delivered_order_count
        ,coalesce(user_order_activity.six_month_net_revenue_percentile) as six_month_net_revenue_percentile
        ,coalesce(user_order_activity.six_month_gross_profit_percentile) as six_month_gross_profit_percentile
        ,coalesce(user_order_activity.twelve_month_net_revenue_percentile) as twelve_month_net_revenue_percentile
        ,coalesce(user_order_activity.lifetime_net_revenue_percentile) as lifetime_net_revenue_percentile
        ,coalesce(user_order_activity.lifetime_paid_order_count_percentile) as lifetime_paid_order_count_percentile
        ,coalesce(user_order_activity.total_california_orders) as total_california_orders
        ,coalesce(user_order_activity.user_average_order_value) as user_average_order_value
        ,coalesce(user_order_activity.twelve_month_japanese_wagyu_revenue)as twelve_month_japanese_wagyu_revenue
        ,coalesce(user_order_activity.lifetime_japanese_wagyu_revenue) as lifetime_japanese_wagyu_revenue
        ,coalesce(user_order_activity.japanese_buyers_club_revenue) as japanese_buyers_club_revenue
        ,coalesce(user_order_activity.moolah_points) as moolah_points
        ,coalesce(user_order_activity.lifetime_awarded_moolah) as lifetime_awarded_moolah
        ,coalesce(user_order_activity.redeemed_moolah_points) as redeemed_moolah_points
        ,user_order_activity.last_paid_membership_order_date
        ,user_order_activity.last_paid_ala_carte_order_date
        ,user_order_activity.last_paid_order_date
        ,user_order_activity.last_paid_membership_order_delivered_date
        ,user_order_activity.last_paid_ala_carte_order_delivered_date
        ,user_order_activity.last_paid_order_delivered_date
        ,user_order_activity.most_recent_paid_order_token
        ,user_order_activity.most_recent_order_promotion_id
        ,user_order_activity.most_recent_order_id
        ,user_order_activity.all_leads
        ,user_order_activity.hot_lead
        ,user_order_activity.warm_lead
        ,user_order_activity.cold_lead
        ,user_order_activity.purchaser
        ,user_order_activity.recent_purchaser
        ,user_order_activity.lapsed_purchaser
        ,user_order_activity.dormant_purchaser
        ,user_order_activity.beef_revenue
        ,user_order_activity.bison_revenue
        ,user_order_activity.chicken_revenue
        ,user_order_activity.japanese_wagyu_revenue
        ,user_order_activity.lamb_revenue
        ,user_order_activity.pork_revenue
        ,user_order_activity.sides_revenue
        ,user_order_activity.turkey_revenue
        ,user_order_activity.wagyu_revenue
        ,user_order_activity.bundle_revenue
        ,user_order_activity.seafood_revenue
        ,user_order_activity.most_recent_beef_order_date   
        ,user_order_activity.most_recent_bison_order_date 
        ,user_order_activity.most_recent_chicken_order_date
        ,user_order_activity.most_recent_japanse_wagyu_order_date
        ,user_order_activity.most_recent_lamb_order_date
        ,user_order_activity.most_recent_pork_order_date
        ,user_order_activity.most_recent_seafood_order_date
        ,user_order_activity.most_recent_sides_order_date
        ,user_order_activity.most_recent_turkey_order_date
        ,user_order_activity.most_recent_wagyu_order_date
        ,user_order_activity.most_recent_bundle_order_date
        ,user_order_activity.last_paid_order_value
        ,user_order_activity.last_paid_moolah_order_date
        ,user_order_activity.last_14_days_impacful_customer_reschedules
        ,user_referrals.referrals_sent
        ,user_referrals.referrals_redeemed
        ,if(user_gift_card_transaction_history.redemption_user_id is not null, true, false) as has_redeemed_gift_card
        ,if(user_gift_card_redemption_vip_affiliate.redemption_user_id is not null, true, false) as has_redeemed_gc_vip_referral
        ,fb_split.fb_test

    from users
        left join user_membership on users.user_id = user_membership.user_id
        left join user_order_activity on users.user_id = user_order_activity.user_id
        left join phone_number on users.phone_number_id = phone_number.phone_number_id
        left join ccpa_users on users.user_token = ccpa_users.user_token
        left join user_visits on users.user_id = user_visits.user_id
        left join postal_code on users.user_zip = cast(postal_code.postal_code as string)
        left join identity on users.user_id = identity.user_id
        left join user_contacts on users.user_token = user_contacts.user_token
        left join user_referrals on users.user_id = user_referrals.user_id
        left join user_gift_card_transaction_history on users.user_id = user_gift_card_transaction_history.redemption_user_id 
        left join user_gift_card_redemption_vip_affiliate on users.user_id = user_gift_card_redemption_vip_affiliate.redemption_user_id 
        left join fb_split on users.user_token = fb_split.user_token
)

,final as (
    select
        user_id
        ,phone_number_id
        
        ,case
            when user_roles_for_access is not null then 'EMPLOYEE'
            when user_email like '%@CROWDCOW.COM' and user_email not like 'TEMPORARY%CROWDCOW.COM' then 'INTERNAL'
            when is_rastellis then 'RASTELLIS'
            when is_qvc then 'QVC'
            when is_seabear then 'SEABEAR'
            when is_backyard_butchers then 'BYB'
            when user_email like 'TEMPORARY%CROWDCOW.COM' then 'GUEST'
            when user_type = 'C' then 'CUSTOMER'
            when user_type = 'P' then 'PLACEHOLDER'
            else 'OTHER'
         end as user_type
        
        ,active_order_id
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
        ,full_contact_id as fc_id
        ,user_zip
        ,user_token
        ,user_referrer_token
        ,user_cow_cash_balance_usd
        ,user_support_status
        ,customer_cohort_date
        ,date_diff(customer_cohort_date,current_date, MONTH) as customer_cohort_tenure_months
        ,membership_cohort_date
        ,date_diff(membership_cohort_date,current_date, MONTH) as membership_cohort_tenure_months
        ,customer_reactivation_date
        ,date_diff(customer_reactivation_date,current_date, MONTH) as customer_reactivation_tenure_months
        ,first_promotion_type
        ,acquisition_promotion_id
        ,acquisition_promotion_source
        ,acquisition_promotion_name
        ,current_promotion_type
        ,current_renew_period
        ,total_membership_count
        ,total_uncancelled_memberships
        ,membership_tenure_months
        ,average_order_frequency_days
        ,average_membership_order_frequency_days
        ,average_ala_carte_order_frequency_days
        ,days_from_ala_carte_to_membership
        ,lifetime_net_revenue
        ,lifetime_net_product_revenue
        ,lifetime_paid_order_count
        ,total_completed_unpaid_uncancelled_orders
        ,total_paid_ala_carte_order_count
        ,total_paid_membership_order_count
        ,last_90_days_paid_membership_order_count
        ,last_30_days_paid_order_count
        ,last_60_days_paid_order_count
        ,last_90_days_paid_order_count
        ,last_120_days_paid_order_count
        ,last_180_days_paid_order_count
        ,six_month_paid_order_count
        ,twelve_month_purchase_count
        ,total_paid_gift_order_count
        ,recent_delivered_order_count
        ,six_month_net_revenue
        ,six_month_gross_profit
        ,twelve_month_net_revenue
        ,six_month_net_revenue_percentile
        ,six_month_gross_profit_percentile
        ,twelve_month_net_revenue_percentile
        ,lifetime_net_revenue_percentile
        ,lifetime_paid_order_count_percentile
        ,total_california_orders
        ,user_average_order_value
        ,twelve_month_japanese_wagyu_revenue
        ,lifetime_japanese_wagyu_revenue
        ,japanese_buyers_club_revenue
        ,lifetime_awarded_moolah
        ,moolah_points
        ,redeemed_moolah_points
        ,total_calls as total_phone_burner_calls
        ,call_result as last_phone_burner_call_result
        ,owner_name as phone_burner_contact_owner
        ,attributed_visit_id
        
        ,nullif(
            greatest(
                coalesce(unsubscribed_all_at_utc,'1970-01-01')
                ,coalesce(email_subscribed_often_at_utc,'1970-01-01')
                ,coalesce(email_subscribed_weekly_at_utc,'1970-01-01')
            )
        ,'1970-01-01') as last_email_preference_date
        
        ,case
            when greatest(
                    coalesce(unsubscribed_all_at_utc,'1970-01-01')
                    ,coalesce(email_subscribed_often_at_utc,'1970-01-01')
                    ,coalesce(email_subscribed_weekly_at_utc,'1970-01-01')
                ) = unsubscribed_all_at_utc 
            then 'UNSUBSCRIBED ALL'
            when greatest(
                    coalesce(unsubscribed_all_at_utc,'1970-01-01')
                    ,coalesce(email_subscribed_often_at_utc,'1970-01-01')
                    ,coalesce(email_subscribed_weekly_at_utc,'1970-01-01')
                ) = email_subscribed_often_at_utc 
                or (
                    unsubscribed_all_at_utc is null 
                    and email_subscribed_often_at_utc is null 
                    and email_subscribed_weekly_at_utc is null
                    and has_opted_in_to_emails
                )
            then 'OFTEN'
            when greatest(
                    coalesce(unsubscribed_all_at_utc,'1970-01-01')
                    ,coalesce(email_subscribed_often_at_utc,'1970-01-01')
                    ,coalesce(email_subscribed_weekly_at_utc,'1970-01-01')
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
        ,not is_active_member_90_day and twelve_month_purchase_count > 0 as is_active_alc
        ,is_banned
        ,is_current_promotion_ffl
        ,is_first_promotion_ffl
        ,is_rastellis
        ,is_qvc
        ,is_seabear
        ,is_backyard_butchers
        ,does_allow_sms
        ,has_opted_in_to_emails
        ,last_call_at_utc is not null as has_phone_burner_contact
        ,cast(most_recent_membership_created_date as date) >= cast(last_call_at_utc as date) as did_create_membership_after_call
        ,last_sign_in_at_utc
        ,last_call_at_utc
        ,first_membership_created_date
        ,most_recent_membership_created_date
        ,most_recent_membership_cancelled_date
        ,last_paid_membership_order_date
        ,last_paid_ala_carte_order_date
        ,cast(last_paid_order_date as date) as last_paid_order_date
        ,last_paid_membership_order_delivered_date
        ,last_paid_ala_carte_order_delivered_date
        ,last_paid_order_delivered_date
        ,most_recent_paid_order_token
        ,most_recent_order_promotion_id
        ,most_recent_order_id
        ,created_at_utc
        ,updated_at_utc
        ,all_leads
        ,hot_lead
        ,warm_lead
        ,cold_lead
        ,purchaser
        ,recent_purchaser
        ,lapsed_purchaser
        ,dormant_purchaser
        ,coalesce(total_paid_ala_carte_order_count > 0 and not is_member, FALSE) as alc_customer
        ,coalesce(total_paid_ala_carte_order_count = 1 and not is_member, FALSE) as new_alc 
        ,coalesce(total_paid_ala_carte_order_count = 1 and (lifetime_japanese_wagyu_revenue/if(lifetime_net_product_revenue = 0,1,lifetime_net_product_revenue)) > 0.5 and not is_member, FALSE) as new_alc_wagyu
        ,coalesce(recent_purchaser and not is_member, FALSE) as recent_alc
        ,coalesce(lapsed_purchaser and not is_member, FALSE) as lapsed_alc
        ,coalesce(dormant_purchaser and not is_member, FALSE) as dormant_alc
        ,coalesce(total_paid_membership_order_count = 1 and is_member and not is_cancelled_member, FALSE ) as new_subscriber
        ,coalesce(total_paid_membership_order_count = 1 and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-1-WEEK','RENEW-PERIOD-2-WEEKS','RENEW-PERIOD-3-WEEKS','RENEW-PERIOD-4-WEEKS','RENEW-PERIOD-MONTHLY'), FALSE ) as new_subscriber_4_weeks
        ,coalesce(total_paid_membership_order_count = 1 and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-6-WEEKS','RENEW-PERIOD-8-WEEKS'), FALSE ) as new_subscriber_5_8_weeks
        ,coalesce(total_paid_membership_order_count = 1 and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-12-WEEKS'), FALSE ) as new_subscriber_12_weeks
        ,coalesce(last_paid_membership_order_date >= DATE_SUB(current_date(), INTERVAL 45 DAY) and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-1-WEEK','RENEW-PERIOD-2-WEEKS','RENEW-PERIOD-3-WEEKS','RENEW-PERIOD-4-WEEKS','RENEW-PERIOD-MONTHLY'), FALSE ) as active_subscriber_4_weeks
        ,coalesce(last_paid_membership_order_date >= DATE_SUB(current_date(), INTERVAL 70 DAY) and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-6-WEEKS','RENEW-PERIOD-8-WEEKS'), FALSE ) as active_subscriber_5_8_weeks
        ,coalesce(last_paid_membership_order_date >= DATE_SUB(current_date(), INTERVAL 100 DAY) and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-12-WEEKS'), FALSE ) as active_subscriber_12_weeks
        ,coalesce(last_paid_membership_order_date <= DATE_SUB(current_date(), INTERVAL 45 DAY) and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-1-WEEK','RENEW-PERIOD-2-WEEKS','RENEW-PERIOD-3-WEEKS','RENEW-PERIOD-4-WEEKS','RENEW-PERIOD-MONTHLY'), FALSE ) as lapsed_subscriber_4_weeks
        ,coalesce(last_paid_membership_order_date <= DATE_SUB(current_date(), INTERVAL 70 DAY) and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-6-WEEKS','RENEW-PERIOD-8-WEEKS'), FALSE ) as lapsed_subscriber_5_8_weeks
        ,coalesce(last_paid_membership_order_date <= DATE_SUB(current_date(), INTERVAL 100 DAY) and is_member and not is_cancelled_member and current_renew_period in ('RENEW-PERIOD-12-WEEKS'), FALSE ) as lapsed_subscriber_12_weeks
        ,coalesce( last_paid_order_date >= cast(most_recent_membership_cancelled_date as date) and is_cancelled_member and cast(most_recent_membership_cancelled_date as date) >= DATE_SUB(current_date(), INTERVAL 90 DAY), FALSE ) as active_cancelled_subscriber 
        ,coalesce( last_paid_order_date <= cast(most_recent_membership_cancelled_date as date) and is_cancelled_member and cast(most_recent_membership_cancelled_date as date) >= DATE_SUB(current_date(), INTERVAL 90 DAY), FALSE ) as recent_cancelled_subscriber 
        ,coalesce( last_paid_order_date <= cast(most_recent_membership_cancelled_date as date) and is_cancelled_member and cast(most_recent_membership_cancelled_date as date) < DATE_SUB(current_date(), INTERVAL 90 DAY), FALSE ) as lapsed_cancelled_subscriber 
        ,beef_revenue
        ,bison_revenue
        ,chicken_revenue
        ,japanese_wagyu_revenue
        ,lamb_revenue
        ,pork_revenue
        ,sides_revenue
        ,turkey_revenue
        ,wagyu_revenue
        ,bundle_revenue
        ,seafood_revenue
        ,cast(most_recent_beef_order_date as date) as most_recent_beef_order_date    
        ,cast(most_recent_bison_order_date as date) as most_recent_bison_order_date  
        ,cast(most_recent_chicken_order_date as date) as most_recent_chicken_order_date 
        ,cast(most_recent_japanse_wagyu_order_date as date) as most_recent_japanse_wagyu_order_date 
        ,cast(most_recent_lamb_order_date as date) as most_recent_lamb_order_date 
        ,cast(most_recent_pork_order_date as date) as most_recent_pork_order_date 
        ,cast(most_recent_seafood_order_date as date) as most_recent_seafood_order_date 
        ,cast(most_recent_sides_order_date as date) as most_recent_sides_order_date 
        ,cast(most_recent_turkey_order_date as date) as most_recent_turkey_order_date 
        ,cast(most_recent_wagyu_order_date as date) as most_recent_wagyu_order_date 
        ,cast(most_recent_bundle_order_date as date) as most_recent_bundle_order_date 
        ,last_paid_order_value
        ,cast(last_paid_moolah_order_date as date) as last_paid_moolah_order_date
        ,last_14_days_impacful_customer_reschedules
        ,referrals_sent
        ,referrals_redeemed
        ,has_redeemed_gift_card
        ,has_redeemed_gc_vip_referral
        ,fb_test


    from user_joins
    where not is_ccpa
)

select * from final

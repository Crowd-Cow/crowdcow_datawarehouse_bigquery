with orders as (select * from {{ ref('orders') }} 
                where (NOT (orders.is_rastellis) OR (orders.is_rastellis) IS NULL)
                  AND (NOT (orders.is_qvc) OR (orders.is_qvc) IS NULL)
                  AND (NOT (orders.is_seabear) OR (orders.is_seabear) IS NULL)
                  AND (NOT (orders.is_backyard_butchers) OR (orders.is_backyard_butchers) IS NULL))
,google_ads as (select * from {{ ref('google_ads_campaign_performance') }} )
,affiliate as (select * from {{ ref('affiliate_orders') }} )
,users as (select * from {{ ref('users') }} )
,user_attribution as (select * from {{ ref('user_attribution') }} )
,discounts as ( select * from {{ ref('discounts') }} )
,gs_manual_changes as ( select * from {{ ref('stg_gs__daily_cac_manual')}})
,meta as ( select * from {{ ref('stg_fb_ads__campaign_report_daily')}})
,fiscal_calendar as (select * from {{ ref('retail_calendar') }} where fiscal_year > 2022) 
,daily_calendar AS (
    SELECT
        date(calendar_date) AS calendar_date
    FROM fiscal_calendar
    WHERE calendar_date <= CURRENT_DATE()
)
,channels as (
    select distinct
        case
            when user_attribution.sub_channel = 'USER REFERRAL' then 'USER REFERRAL'
            when user_attribution.sub_channel = 'NON-USER REFERRAL' then 'NON-USER REFERRAL'
            when user_attribution.channel is null then 'OTHER'
        else user_attribution.channel end as attribution_channel
    from user_attribution
)
,date_channel as ( 
    select 
        calendar_date,
        attribution_channel
    from daily_calendar
    cross join channels
)


,meta_spend as (
    select 
        date_start,
        sum(spend) as spend
    from meta
    group by 1 
)

,google_ads_spend as (
    select
    date(timestamp(campaign_stat_date_utc)) as campaign_start_date_utc
    ,sum(TOTAL_COST_USD) as total_cost_usd
    from google_ads
    group by 1
)

,affiliate_spend as (
    select
        date(timestamp(transaction_date_utc)) as transaction_date_utc
        ,COALESCE(SUM((transaction_amount * 0.04) + comission + shareasale_comission), 0) AS shareasale_orders_total_cost,
    from affiliate
    group by 1
)

,user_referral as (
    select
    date(created_at_utc, 'America/Los_Angeles') as created_at_utc
    ,COALESCE(SUM(discounts.DISCOUNT_USD), 0) AS discounts_total_discount_amount
    from orders
    left join discounts ON orders.order_id = discounts.order_id
    where
        orders.paid_order_rank = 1
      AND (NOT orders.is_cancelled_order OR orders.is_cancelled_order IS NULL)
      AND (orders.is_paid_order)
      AND (NOT orders.is_rastellis OR orders.is_rastellis IS NULL)
      AND (NOT orders.is_qvc OR orders.is_qvc IS NULL) AND (NOT orders.is_seabear OR orders.is_seabear IS NULL)
      AND (NOT orders.is_backyard_butchers OR orders.is_backyard_butchers IS NULL)
      AND (discounts.business_group = 'ACQUISITION MARKETING - MEMBER REFERRAL')
    group by 1

)

,attribution as (
select
     DATE(orders.order_paid_at_utc, 'America/Los_Angeles') as order_paid_at_utc
    ,case
        --when (user_attribution.channel != "SEM" AND user_attribution.channel != "SEO" AND user_attribution.channel != "REFERRAL" AND user_attribution.channel != "AFFILIATE" AND discounts.business_group != "GIFT CARD REDEMPTION" ) AND orders.paid_order_rank = 1 AND orders.stripe_card_brand = "AMERICAN EXPRESS" then "AMEX"
        when user_attribution.sub_channel = 'USER REFERRAL' then 'USER REFERRAL'
        when user_attribution.sub_channel = 'NON-USER REFERRAL' then 'NON-USER REFERRAL'
        when user_attribution.channel is null then 'OTHER'
        else user_attribution.channel end as attribution_channel
    ,COUNT(DISTINCT CASE WHEN (orders.PAID_ORDER_RANK  = 1) AND (NOT COALESCE(orders.IS_CANCELLED_ORDER , FALSE)) THEN orders.user_id  ELSE NULL END) AS orders_new_paid_customers
    ,COUNT(DISTINCT case when orders.paid_membership_order_rank = 1 and orders.paid_order_rank = 1 and not orders.is_cancelled_order then orders.user_id end ) AS orders_new_paid_membership_customers
    ,COUNT(DISTINCT CASE WHEN orders.IS_PAID_ORDER AND (NOT COALESCE(orders.IS_CANCELLED_ORDER , FALSE)) THEN orders.user_id  ELSE NULL END) AS orders_total_paid_customers
from orders
left join users AS users ON orders.USER_ID = users.USER_ID
left join user_attribution AS user_attribution ON users.attributed_visit_id = user_attribution.VISIT_ID
left join discounts ON orders.order_id = discounts.order_id
group by 1,2
)

,all_channels as (

    select 
        calendar_date as order_paid_at_utc,
        date_channel.attribution_channel,
        orders_new_paid_customers,
        orders_new_paid_membership_customers,
        orders_total_paid_customers
    from date_channel
    left join attribution on attribution.order_paid_at_utc =  date_channel.calendar_date and date_channel.attribution_channel = attribution.attribution_channel
)

--- CTE to Remove/add changes from/to direct 
,manual_changes as (
    select
        date
        ,channel
        ,action
        ,amount
        ,new_customers
        ,orders_new_paid_customers as original_new_customers
    from all_channels
    left join gs_manual_changes on order_paid_at_utc = gs_manual_changes.date and attribution_channel = gs_manual_changes.channel
)

,final as ( 
select
   timestamp(order_paid_at_utc, 'America/Los_Angeles') as order_paid_at_utc
     ,attribution_channel 
     ,case 
        when attribution_channel = 'DIRECT' then coalesce(orders_new_paid_customers,0) + coalesce(substract_manual.original_new_customers,0) - coalesce(substract_manual.new_customers,0)
        when include_manual.action = 'REPLACE' then coalesce(include_manual.new_customers, orders_new_paid_customers) 
        when include_manual.action = 'ADD' then coalesce(include_manual.new_customers,0) +  orders_new_paid_customers
      else orders_new_paid_customers end as orders_new_paid_customers
     ,orders_new_paid_membership_customers
     ,orders_total_paid_customers
     ,case 
        when include_manual.action = 'REPLACE' then include_manual.amount
        when include_manual.action = 'ADD' then coalesce(shareasale_orders_total_cost,total_cost_usd,discounts_total_discount_amount*2,meta_spend.spend) + include_manual.amount
      else coalesce(shareasale_orders_total_cost,total_cost_usd,discounts_total_discount_amount*2,meta_spend.spend) end as spend
     ,shareasale_orders_total_cost as affiliate_spend
     ,total_cost_usd as sem_spend
     ,discounts_total_discount_amount * 2 as referral_spend
     ,meta_spend.spend as meta_spend
from all_channels
left join google_ads_spend on order_paid_at_utc = google_ads_spend.campaign_start_date_utc and attribution_channel = 'SEM'
left join affiliate_spend on  order_paid_at_utc = affiliate_spend.transaction_date_utc and attribution_channel = 'AFFILIATE'
left join user_referral on order_paid_at_utc = user_referral.created_at_utc and attribution_channel = 'USER REFERRAL'
left join manual_changes as include_manual on order_paid_at_utc = include_manual.date and attribution_channel = include_manual.channel
left join manual_changes as substract_manual on order_paid_at_utc = substract_manual.date and attribution_channel = 'DIRECT' and substract_manual.channel = 'AMEX'
left join meta_spend on order_paid_at_utc = meta_spend.date_start and attribution_channel = 'SOCIAL'
)



select 
      final.order_paid_at_utc
     ,final.attribution_channel 
     ,case 
        when final.attribution_channel = 'DIRECT' and final.orders_new_paid_customers < 0  then 0
        when final.attribution_channel = 'AMEX'  then final.orders_new_paid_customers + COALESCE(f.orders_new_paid_customers, 0)
      else final.orders_new_paid_customers end as orders_new_paid_customers
     ,final.orders_new_paid_membership_customers
     ,final.orders_total_paid_customers
     ,final.spend
     ,final.affiliate_spend
     ,final.sem_spend
     ,final.referral_spend
     ,final.meta_spend
from final
left join final as f on final.order_paid_at_utc = f.order_paid_at_utc  and f.orders_new_paid_customers < 0 and f.attribution_channel = 'DIRECT'


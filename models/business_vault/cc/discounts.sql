with

credit as ( select * from {{ ref('credits') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,promotion as ( select * from {{ ref('promotions') }} )

,union_discounts as (
    select
        'CREDITS' as discount_source
        ,credit_id as discount_id
        ,promotion_id
        ,promotion_source
        ,order_id
        ,credit_business_group as business_group
        ,credit_financial_account as financial_account
        ,credit_description
        ,awarded_cow_cash_message
        ,credit_discount_usd as discount_usd
        ,created_at_utc
        ,updated_at_utc
    from credit
    where order_id is not null --this will select only order level credits. Item level credits are added in the item discounts below under the "OTHER ITEM LEVEL PROMOTIONS" business_group

    union all 

    select
        'ITEM DISCOUNTS' as discount_source
        ,bid_id as discount_id
    
        ,case
            when discounts.index = 2 then promotion_id else null
         end as promotion_id

        ,promotion_source
    
        ,order_id

        ,case
            when discounts.index = 0 then 'MEMBERSHIP 5%'
            when discounts.index = 1 then 'MERCHANDISING DISCOUNT'
            when discounts.index = 2 and promotion_id in (18,20,22,35,37) and promotion_source = 'PROMOTION' then 'MEMBERSHIP FREE PROTEIN PROMOTIONS'
            when discounts.index = 2 and promotion_id is not null then 'OTHER ITEM LEVEL PROMOTIONS'
         end as business_group

        ,case
            when discounts.index = 0 then '41303 - SUBSCRIPTION REWARDS'
            when discounts.index = 1 then '41300 - MERCH DISCOUNTS - RETAIL'
            when discounts.index = 2 and promotion_id is not null then '41301 - NEW CUSTOMER SUBSCRIPTIONS'
         end as financial_account

        ,null::text as credit_description
        ,null::text as awarded_cow_cash_message
        ,round(discounts.value,2) as discount_usd
        ,created_at_utc
        ,updated_at_utc
    from order_item,
        lateral flatten(
        array_construct(
            order_item.item_member_discount
            ,order_item.item_merch_discount
            ,order_item.item_free_protein_discount + order_item.item_promotion_discount
        )
    ) as discounts
)

,add_promotion_type as (
    select
        {{ dbt_utils.surrogate_key( ['discount_source','discount_id','order_id','business_group'] ) }} as discount_detail_id
        ,union_discounts.discount_source
        ,union_discounts.discount_id
        ,union_discounts.promotion_id
        ,promotion.promotion_type
        ,union_discounts.order_id
        ,union_discounts.business_group
        ,union_discounts.financial_account
        ,union_discounts.credit_description
        ,union_discounts.awarded_cow_cash_message
        ,union_discounts.discount_usd
        ,coalesce(promotion.is_new_member_promotion,FALSE) as is_new_member_promotion
        ,union_discounts.created_at_utc
        ,union_discounts.updated_at_utc
    from union_discounts
        left join promotion on union_discounts.promotion_id = promotion.promotion_id
            and union_discounts.promotion_source = promotion.promotion_source
    where business_group is not null
)

,group_by_revenue_bucket as (
    select 
        *
        
        ,case
            when business_group = 'FREE SHIPPING' then 'FREE SHIPPING DISCOUNT'
            when business_group = 'MEMBERSHIP 5%' then 'MEMBERSHIP DISCOUNT'
            when business_group = 'MERCHANDISING DISCOUNT' then 'MERCH DISCOUNT'
            when business_group = 'MEMBERSHIP FREE PROTEIN PROMOTIONS' then 'FREE PROTEIN PROMOTION'
            when business_group = 'OTHER ITEM LEVEL PROMOTIONS' then 'OTHER ITEM LEVEL PROMOTIONS'
            when business_group in ('ACQUISITION MARKETING - PROMOTION CREDITS','MEMBERSHIP PROMOTIONS','OTHER ITEM LEVEL PROMOTIONS')
                and is_new_member_promotion then 'NEW MEMBER DISCOUNT'
            when business_group in ('GIFT CARD REDEMPTION') then 'GIFT REDEMPTION'
            when business_group in ('ACQUISITION MARKETING - GIFT', 'ACQUISITION MARKETING - INFLUENCER','ACQUISITION MARKETING - MEMBER REFERRAL'
                    ,'ACQUISITION MARKETING - PROMOTION CREDITS','CARE CREDITS','OTHER - UNKNOWN','RETENTION MARKETING','REPLACEMENTS')
                    and not is_new_member_promotion then 'OTHER DISCOUNT'
            else 'EXCLUDED FROM REVENUE BUCKETS'
        end as revenue_waterfall_bucket
    from add_promotion_type 
)

select
    discount_detail_id
    ,discount_source
    ,discount_id
    ,promotion_id
    ,promotion_type
    ,order_id
    ,business_group
    ,financial_account
    ,credit_description
    ,awarded_cow_cash_message
    ,revenue_waterfall_bucket
    ,discount_usd
    ,is_new_member_promotion
    ,created_at_utc
    ,updated_at_utc
from group_by_revenue_bucket

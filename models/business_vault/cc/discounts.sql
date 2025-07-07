with

credit as ( select * from {{ ref('credits') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,promotion as ( select * from {{ ref('promotions') }} )
,promotions_claims as ( select distinct promotions_promotion_id, order_id, promo_code from {{ ref('stg_cc__promotions_claims') }} where claimed_at_utc is not null and unclaimed_at_utc is null and promo_code is not null )

,union_discounts as (
SELECT
    'CREDITS' AS discount_source,
    credit_id AS discount_id,
    promotion_id,
    promotion_source,
    order_id,
    credit_business_group AS business_group,
    credit_financial_account AS financial_account,
    credit_description,
    awarded_cow_cash_message,
    credit_discount_usd AS discount_usd,
    created_at_utc,
    updated_at_utc
FROM
    credit
WHERE
    order_id IS NOT NULL

UNION ALL

SELECT
    'ITEM DISCOUNTS' AS discount_source,
    bid_id AS discount_id,
    CASE
        WHEN discounts.offset = 2 THEN promotion_id ELSE NULL
    END AS promotion_id,
    promotion_source,
    order_id,
    CASE
        WHEN discounts.offset = 0 THEN 'MEMBERSHIP 5%'
        WHEN discounts.offset = 1 THEN 'MERCHANDISING DISCOUNT'
        when (discounts.offset = 2 and promotion_id in (18,20,22,35,37) and promotion_source = 'PROMOTION' )
                or (discounts.offset = 2 and promotion_id in (157,186,219,253,259, 285, 286, 287, 299, 300, 301, 439, 440, 441, 448, 449) and promotion_source = 'PROMOTIONS::PROMOTION' ) 
                  then 'MEMBERSHIP FREE PROTEIN PROMOTIONS'
        WHEN discounts.offset = 2 AND promotion_id IS NOT NULL THEN 'OTHER ITEM LEVEL PROMOTIONS'
    END AS business_group,
    CASE
        WHEN discounts.offset = 0 THEN '41303 - SUBSCRIPTION REWARDS'
        WHEN discounts.offset = 1 THEN '41300 - MERCH DISCOUNTS - RETAIL'
        WHEN discounts.offset = 2 AND promotion_id IS NOT NULL THEN '41301 - NEW CUSTOMER SUBSCRIPTIONS'
    END AS financial_account,
    NULL AS credit_description,
    NULL AS awarded_cow_cash_message,
    ROUND(discounts.value, 2) AS discount_usd,
    created_at_utc,
    updated_at_utc
FROM
    order_item,
    UNNEST(ARRAY[
        STRUCT(order_item.item_member_discount AS value, 0 AS offset),
        STRUCT(order_item.item_merch_discount AS value, 1 AS offset),
        STRUCT(order_item.item_free_protein_discount + order_item.item_promotion_discount AS value, 2 AS offset)
    ]) AS discounts
)

,add_promotion_type as (
    select
        {{ dbt_utils.surrogate_key( ['discount_source','discount_id','order_id','business_group'] ) }} as discount_detail_id
        ,union_discounts.discount_source
        ,union_discounts.discount_id
        ,union_discounts.promotion_id
        ,union_discounts.promotion_source
        ,promotion.promotion_type
        ,promotion.promotion_key_value
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
            when (business_group = 'OTHER ITEM LEVEL PROMOTIONS' and promotion_key_value = 'REWARDS_PROGRAM_MOOLAH') then 'MOOLAH ITEM DISCOUNT'
            when  business_group = 'MOOLAH' then 'MOOLAH ORDER DISCOUNT'
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
    group_by_revenue_bucket.discount_detail_id
    ,group_by_revenue_bucket.discount_source
    ,group_by_revenue_bucket.discount_id
    ,group_by_revenue_bucket.promotion_id
    ,group_by_revenue_bucket.promotion_source
    ,group_by_revenue_bucket.promotion_type
    ,group_by_revenue_bucket.order_id
    ,group_by_revenue_bucket.business_group
    ,group_by_revenue_bucket.financial_account
    ,group_by_revenue_bucket.credit_description
    ,group_by_revenue_bucket.awarded_cow_cash_message
    ,group_by_revenue_bucket.revenue_waterfall_bucket
    ,group_by_revenue_bucket.discount_usd
    ,group_by_revenue_bucket.is_new_member_promotion
    ,promotions_claims.promo_code
    ,group_by_revenue_bucket.created_at_utc
    ,group_by_revenue_bucket.updated_at_utc
from group_by_revenue_bucket
    left join promotions_claims 
     on promotions_claims.promotions_promotion_id = group_by_revenue_bucket.promotion_id 
     and promotions_claims.order_id = group_by_revenue_bucket.order_id 

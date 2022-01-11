with

credit as ( select * from {{ ref('credits') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,promotion as ( select * from {{ ref('promotions') }} )

,union_discounts as (
    select
        'CREDITS' as discount_source
        ,credit_id as discount_id
        ,promotion_id
        ,order_id
        ,credit_business_group as business_group
        ,credit_financial_account as financial_account
        ,credit_discount_usd as discount_usd
        ,created_at_utc
        ,updated_at_utc
    from credit

    union all 

    select
        'ITEM DISCOUNTS' as discount_source
        ,bid_id as discount_id
    
        ,case
            when discounts.index = 2 then promotion_id else null
         end as promotion_id
    
        ,order_id

        ,case
            when discounts.index = 0 then 'MEMBERSHIP 5%'
            when discounts.index = 1 then 'MERCHANDISING DISCOUNT'
            when discounts.index = 2 and promotion_id in (18,20,22) then 'MEMBERSHIP PROMOTIONS'
            when discounts.index = 2 and promotion_id not in (18,20,22) then 'OTHER ITEM LEVEL PROMOTIONS'
         end as business_group

        ,case
            when discounts.index = 0 then '41303 - SUBSCRIPTION REWARDS'
            when discounts.index = 1 then '41300 - MERCH DISCOUNTS - RETAIL'
            when discounts.index = 2 and promotion_id is not null then '41301 - NEW CUSTOMER SUBSCRIPTIONS'
         end as financial_account

        ,round(discounts.value,2) as discount_usd
        ,created_at_utc
        ,updated_at_utc
    from order_item,
        lateral flatten(array_construct(order_item.item_member_discount,order_item.item_merch_discount,order_item.item_promotion_discount)) as discounts
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
        ,union_discounts.discount_usd
        ,promotion.is_new_member_promotion
        ,union_discounts.created_at_utc
        ,union_discounts.updated_at_utc
    from union_discounts
        left join promotion on union_discounts.promotion_id = promotion.promotion_id
    where business_group is not null
)

select * from add_promotion_type
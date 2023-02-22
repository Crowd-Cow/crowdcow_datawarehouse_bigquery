with rewards as (select * from {{ ref('stg_cc__reward_points') }} where rewards_program = 'MOOLAH')
	,discount_applied as (select * from {{ ref('discounts') }} where revenue_waterfall_bucket in ('MOOLAH ITEM DISCOUNT','MOOLAH ORDER DISCOUNT'))
	,promos as (select * from {{ ref('promotions') }} where promotion_key_value = 'REWARDS_PROGRAM_MOOLAH')

,redeemed as (
		select reward_point_id
    	,created_at_utc
        ,user_id
        ,giver_id
        ,order_id
        ,reward_reason
        ,reward_spend_amount*100 as total_points_awarded
    from rewards
    where reward_spend_amount < 0
    order by user_id,created_at_utc
)

,redemption_details as (
	select 
    	redeemed.reward_point_id
		,discount_applied.discount_detail_id
		,discount_applied.discount_source
		,discount_applied.discount_id
		,discount_applied.promotion_id
		,discount_applied.promotion_source
		,discount_applied.promotion_type
		,discount_applied.business_group
		,discount_applied.revenue_waterfall_bucket
		,discount_applied.discount_usd
    from redeemed
    	join discount_applied on redeemed.order_id = discount_applied.order_id
)

,balance_calcs as (
	select reward_point_id
    	,created_at_utc
        ,user_id
        ,giver_id
        ,order_id
        ,reward_reason
        ,reward_spend_amount*100 as point_change
    	,sum(reward_spend_amount*100) over(partition by user_id order by created_at_utc) as balance
        ,case when giver_id is not null then true else false end as was_outside_of_purchase
    from rewards
)

select balance_calcs.*
	,promotion_id
    ,promotion_source
    ,promotion_type
    ,discount_usd
from balance_calcs
	left join redemption_details on balance_calcs.reward_point_id = redemption_details.reward_point_id
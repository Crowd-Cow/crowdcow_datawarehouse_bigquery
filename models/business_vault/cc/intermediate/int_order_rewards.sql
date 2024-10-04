with

reward as ( select * from {{ ref('stg_cc__reward_points') }} )
,orders as ( select * from {{ ref('stg_cc__orders') }} where not is_rastellis and not is_qvc)

,moolah_available as (
    select 
        reward.user_id
        ,orders.order_id
        ,order_paid_at_utc
        ,sum(reward_spend_amount*100) as moolah_available_at_purchase
    from reward
        left join orders on reward.user_id = orders.user_id
                                   and reward.created_at_utc < orders.order_paid_at_utc
    where reward.rewards_program = 'MOOLAH'
    group by 1, 2, 3
)

,reward_point_totals as (
    select
        reward.order_id
        ,sum(if(rewards_program = 'WAGYU_CLUB',reward_spend_amount,0)) as jwagyu_reward_spend
        ,(sum(if(rewards_program = 'MOOLAH',reward_spend_amount,0))*100) as moolah_points
        ,(sum(if(rewards_program = 'MOOLAH' and reward_spend_amount>0,reward_spend_amount,0))*100) as total_awarded_moolah
        ,(sum(if(rewards_program = 'MOOLAH' and reward_spend_amount<0,reward_spend_amount,0))*100) as total_moolah_redeemed
    from reward
    group by 1
)

select
    orders.order_id
    ,(moolah_available_at_purchase + coalesce(-total_moolah_redeemed,0)) as moolah_available_for_order
    ,jwagyu_reward_spend
    ,moolah_points
    ,total_awarded_moolah
    ,total_moolah_redeemed
from orders
    left join moolah_available on orders.order_id = moolah_available.order_id
    left join reward_point_totals on orders.order_id = reward_point_totals.order_id

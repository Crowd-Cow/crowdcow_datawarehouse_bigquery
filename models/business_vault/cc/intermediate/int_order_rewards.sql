with

reward as ( select * from {{ ref('stg_cc__reward_points') }} )

select
    order_id
    ,sum(iff(rewards_program = 'WAGYU_CLUB',reward_spend_amount,0)) as jwagyu_reward_spend
    ,(sum(iff(rewards_program = 'MOOLAH',reward_spend_amount,0))*100)::int as moolah_points
    ,(sum(iff(rewards_program = 'MOOLAH' and reward_spend_amount>0,reward_spend_amount,0))*100)::int as total_awarded_moolah
    ,(sum(iff(rewards_program = 'MOOLAH' and reward_spend_amount<0,reward_spend_amount,0))*100)::int as total_moolah_redeemed
from reward
group by 1

with

pick_pack as ( select * from {{ ref('int_pick_pack_durations') }} )
,order_details as (select order_id, is_rastellis, is_qvc from {{ ref('stg_cc__orders')}})

,combined_pick_pack_orders as (
select 
    {{ dbt_utils.surrogate_key(['user_id','pick_pack.order_id','action','action_date']) }} as packing_action_id
    ,order_details.is_rastellis
    ,order_details.is_qvc
    ,pick_pack.* 
from pick_pack
    left join order_details on pick_pack.order_id = order_details.order_id
)

select *
from combined_pick_pack_orders
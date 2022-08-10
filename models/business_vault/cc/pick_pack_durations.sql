with

pick_pack as ( select * from {{ ref('int_pick_pack_durations') }} )

select 
    {{ dbt_utils.surrogate_key(['user_id','order_id','action','action_date']) }} as packing_action_id
    ,{{ get_order_type('pick_pack') }} as is_rastellis
    ,* 
from pick_pack

with

gs as ( select * from {{ ref('stg_gs__dry_ice_feedback') }} )

,final as (
  select
     date_received
    ,order_number
    ,did_the_box_show_any_signs_of_damage_::boolean as did_the_box_show_any_signs_of_damage
    ,did_you_order_arrive_frozen_::boolean as did_you_order_arrive_frozen
    ,how_much_dry_ice_was_left_in_the_box
  
  from gs
)

select * from final
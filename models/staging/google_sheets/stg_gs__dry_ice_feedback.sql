with

form as ( select * from {{ source('google_sheets', 'dry_ice_feedback') }} )

,final as (
  select
     TO_DATE(timestamp, 'MM/DD/YYYY HH24:MI:SS')  as date_received
    ,{{ clean_strings('order_number') }} as order_number
    ,{{ clean_strings('did_the_box_show_any_signs_of_damage_') }} as did_the_box_show_any_signs_of_damage_
    ,{{ clean_strings('did_you_order_arrive_frozen_') }} as did_you_order_arrive_frozen_
    ,{{ clean_strings('how_much_dry_ice_was_left_in_the_box_please_choose_the_closest_option_from_the_answers_pictographs_below_')}} as how_much_dry_ice_was_left_in_the_box
  
  from form
)

select * from final

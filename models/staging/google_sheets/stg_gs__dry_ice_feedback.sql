with

form as ( select * from {{ source('google_sheets', 'dry_ice_feedback') }} )

,final as (
  select
     CAST(timestamp as DATE)  as date_received
    ,{{ clean_strings('order_number') }} as order_number
    ,{{ clean_strings('did_the_box_show_any_signs_of_damage_question_mark') }} as did_the_box_show_any_signs_of_damage
    ,{{ clean_strings('did_you_order_arrive_frozen_question_mark') }} as did_you_order_arrive_frozen
    ,{{ clean_strings('how_much_dry_ice_was_left_in_the_box_question_mark___newline_please_choose_the_closest_option_f_f45e0eff16cde02c3c8650139ce5d131')}} as how_much_dry_ice_was_left_in_the_box
  
  from form
)

select * from final

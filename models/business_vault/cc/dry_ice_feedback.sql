with

gs as ( select * from {{ ref('stg_gs__dry_ice_feedback') }} )

,final as (
  select
     date_received
    ,order_number
    ,CAST(case when did_the_box_show_any_signs_of_damage = 'YES' then TRUE else FALSE END AS BOOLEAN) AS did_the_box_show_any_signs_of_damage
    ,CAST(case when did_you_order_arrive_frozen = 'YES' then TRUE else FALSE END AS BOOLEAN) AS did_you_order_arrive_frozen
    ,how_much_dry_ice_was_left_in_the_box
  
  from gs
)

select * from final
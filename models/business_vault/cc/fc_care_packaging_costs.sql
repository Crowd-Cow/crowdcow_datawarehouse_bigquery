with static_costs as (select * from {{ ref('stg_gs__fc_care_packaging_costs') }} )
     ,fc_details as (select * from {{ ref('stg_cc__fcs') }} )
     ,box_details as (select * from {{ ref('stg_cc__box_types') }} )

,all_combined as (
    select static_costs.fc_name
           ,fc_details.fc_id
           ,static_costs.box_type
           ,static_costs.month_of_costs
           ,static_costs.cost_usd
           ,static_costs.cost_type
           ,static_costs.labor_hours
           ,box_details.box_id
           ,box_details.height_in_inches as box_height_in_inches
           ,box_details.width_in_inches as box_width_in_inches
    from static_costs
        left join fc_details on static_costs.fc_name = fc_details.fc_name
        left join box_details on static_costs.box_type = box_details.box_name
                              and fc_details.fc_id = box_details.fc_id
)


select fc_name
    ,fc_id
    ,box_type
    ,month_of_costs
    ,cost_usd
    ,cost_type
    ,labor_hours
    ,box_id
    ,box_height_in_inches
    ,box_width_in_inches
from all_combined
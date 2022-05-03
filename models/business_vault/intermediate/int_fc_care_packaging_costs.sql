with static_costs as (select * from ref('stg_gs__fc_care_packaging_costs') }} )
     ,fc_details as (select * from {{ ref('stg_cc__fcs') }} )
     ,box_details as (select * from {{ ref('stg_cc__box_types') }} )

,fc_ids as (
    select static_costs.fc_name
           ,fc_details.id as fc_id
           ,static_costs.month_of_costs
           ,static_costs.box_type
           ,static_costs.cost_usd
           ,static_costs.cost_type
           ,static_costs.labor_hours
    from static_costs
        left join fc_details on static_costs.fc_name = fc_details.name
)

,box_ids as (
    select fc_ids.*
           ,box_details.*
    from fc_ids
        left join box_details on fc_ids.box_type = box_details.name
                              and fc_ids.fc_id = box_details.fc_id
)

select *
from box_ids
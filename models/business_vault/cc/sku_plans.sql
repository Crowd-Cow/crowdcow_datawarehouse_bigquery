with

sku_plan as ( select * from {{ ref('stg_cc__sku_plans') }} )
,sku_plan_entry as ( select * from {{ ref('stg_cc__sku_plan_entries') }} )
,cut as ( select * from {{ ref('stg_cc__cuts') }} )

,join_entries as (
    select
        sku_plan.sku_plan_id
        ,sku_plan.sku_plan_type
        ,sku_plan.sku_plan_name
        ,sku_plan.created_at_utc
        ,sku_plan.updated_at_utc
        ,sku_plan_entry.cut_id
    from sku_plan
        left join sku_plan_entry on sku_plan.sku_plan_id = sku_plan_entry.sku_plan_id
)

,get_cut_key as (
    select distinct
        join_entries.*
        ,cut.cut_key
    from join_entries
        left join cut on join_entries.cut_id = cut.cut_id
            and join_entries.created_at_utc >= cut.adjusted_dbt_valid_from
            and join_entries.created_at_utc <= cut.adjusted_dbt_valid_to
)

select
    {{ dbt_utils.surrogate_key(['sku_plan_id','cut_id']) }} as sku_plan_cut_id
    ,get_cut_key.*
from get_cut_key

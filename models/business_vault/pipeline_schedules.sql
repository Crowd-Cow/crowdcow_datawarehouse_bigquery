with

pipeline_schedule as ( select * from {{ ref('stg_cc__pipeline_schedules') }} )
,actor as ( select * from {{ ref('stg_cc__pipeline_actors') }} )
,storage as ( select * from {{ ref('stg_cc__offsite_storages') }} )

,most_recent_schedule_values as (
    select
        pipeline_order_id
        ,schedule_type
        ,last_value(proposed_date) over(partition by pipeline_order_id,schedule_type order by pipeline_schedule_id) as last_proposed_date
        ,last_value(actual_date) over(partition by pipeline_order_id,schedule_type order by pipeline_schedule_id) as last_actual_date
        ,last_value(pipeline_actor_id) over(partition by pipeline_order_id,schedule_type order by pipeline_schedule_id) as last_pipeline_actor_id
        ,last_value(status) over(partition by pipeline_order_id,schedule_type order by pipeline_schedule_id) as last_pipeline_status
        ,last_value(offsite_storage_id) over(partition by pipeline_order_id,schedule_type order by pipeline_schedule_id) as last_offsite_storage_id
        ,row_number() over(partition by pipeline_order_id,schedule_type order by pipeline_schedule_id desc) as rn
    from pipeline_schedule
    qualify rn = 1
)

,schedule_joins as (
    select
        most_recent_schedule_values.*
        ,actor.actor_id
        ,actor.pipeline_actor_name
        ,storage.offsite_storage_name
    from most_recent_schedule_values
        left join actor on most_recent_schedule_values.last_pipeline_actor_id = actor.pipeline_actor_id
        left join storage on most_recent_schedule_values.last_offsite_storage_id = storage.offsite_storage_id
)

,pivot_schedule_types as (
    select 
        pipeline_order_id
        ,max(case when schedule_type = 'FARM_OUT' then last_proposed_date end) as farm_out_proposed_date
        ,max(case when schedule_type = 'FARM_OUT' then last_actual_date end) as farm_out_actual_date
        ,max(case when schedule_type = 'FARM_OUT' then actor_id end) as farm_id
        ,max(case when schedule_type = 'FARM_OUT' then pipeline_actor_name end) as farm_out_name
        ,max(case when schedule_type = 'FARM_OUT' then last_pipeline_status end) as farm_out_status
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_proposed_date end) as slaughter_kill_proposed_date
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_actual_date end) as slaughter_kill_actual_date
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then pipeline_actor_name end) as slaughter_kill_name
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_pipeline_status end) as slaughter_kill_status 
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_proposed_date end) as slaughter_out_proposed_date
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_actual_date end) as slaughter_out_actual_date
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then pipeline_actor_name end) as slaughter_out_name
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_pipeline_status end) as slaughter_out_status
        ,max(case when schedule_type = 'PACKER_IN' then last_proposed_date end) as packer_in_proposed_date
        ,max(case when schedule_type = 'PACKER_IN' then last_actual_date end) as packer_in_actual_date
        ,max(case when schedule_type = 'PACKER_IN' then pipeline_actor_name end) as packer_in_name
        ,max(case when schedule_type = 'PACKER_IN' then last_pipeline_status end) as packer_in_status
        ,max(case when schedule_type = 'PACKER_OUT' then last_proposed_date end) as packer_out_proposed_date
        ,max(case when schedule_type = 'PACKER_OUT' then last_actual_date end) as packer_out_actual_date
        ,max(case when schedule_type = 'PACKER_OUT' then pipeline_actor_name end) as packer_out_name
        ,max(case when schedule_type = 'PACKER_OUT' then last_pipeline_status end) as packer_out_status
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_proposed_date end) as processor_in_proposed_date
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_actual_date end) as processor_in_actual_date
        ,max(case when schedule_type = 'PROCESSOR_IN' then pipeline_actor_name end) as processor_in_name
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_pipeline_status end) as processor_in_status
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_proposed_date end) as processor_out_proposed_date
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_actual_date end) as processor_out_actual_date
        ,max(case when schedule_type = 'PROCESSOR_OUT' then pipeline_actor_name end) as processor_out_name
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_pipeline_status end) as processor_out_status
        ,max(case when schedule_type = 'PROCESSOR_OUT' then offsite_storage_name end) as processor_out_offsite_storage_name
        ,max(case when schedule_type = 'FC_IN' then last_proposed_date end) as fc_in_proposed_date
        ,max(case when schedule_type = 'FC_IN' then last_actual_date end) as fc_in_actual_date
        ,max(case when schedule_type = 'FC_IN' then actor_id end) as fc_in_fc_id
        ,max(case when schedule_type = 'FC_IN' then pipeline_actor_name end) as fc_in_name
        ,max(case when schedule_type = 'FC_IN' then last_pipeline_status end) as fc_in_status
        ,max(case when schedule_type = 'FC_IN' then offsite_storage_name end) as fc_in_offsite_storage_name
        ,max(case when schedule_type = 'FC_SCAN' then last_proposed_date end) as fc_scan_proposed_date
        ,max(case when schedule_type = 'FC_SCAN' then last_actual_date end) as fc_scan_actual_date
        ,max(case when schedule_type = 'FC_SCAN' then actor_id end) as fc_scan_fc_id
        ,max(case when schedule_type = 'FC_SCAN' then pipeline_actor_name end) as fc_scan_name
        ,max(case when schedule_type = 'FC_SCAN' then last_pipeline_status end) as fc_scan_status
    from schedule_joins
    group by 1
)

select * from pivot_schedule_types

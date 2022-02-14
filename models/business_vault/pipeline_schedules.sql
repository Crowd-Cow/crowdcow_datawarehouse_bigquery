with

pipeline_schedule as ( select * from {{ ref('stg_cc__pipeline_schedules') }} )
,actor as ( select * from {{ ref('stg_cc__pipeline_actors') }} )

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

,pivot_schedule_types as (
    select 
        pipeline_order_id
        ,max(case when schedule_type = 'FARM_OUT' then last_proposed_date end) as farm_out_proposed_date
        ,max(case when schedule_type = 'FARM_OUT' then last_actual_date end) as farm_out_actual_date
        ,max(case when schedule_type = 'FARM_OUT' then last_pipeline_actor_id end) as farm_out_actor_id
        ,max(case when schedule_type = 'FARM_OUT' then last_pipeline_status end) as farm_out_status  
        ,max(case when schedule_type = 'FARM_OUT' then last_offsite_storage_id end) as farm_out_offsite_storage_id
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_proposed_date end) as slaughter_kill_proposed_date
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_actual_date end) as slaughter_kill_actual_date
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_pipeline_actor_id end) as slaughter_kill_actor_id
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_pipeline_status end) as slaughter_kill_status 
        ,max(case when schedule_type = 'SLAUGHTER_KILL' then last_offsite_storage_id end) as slaughter_kill_offsite_storage_id
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_proposed_date end) as slaughter_out_proposed_date
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_actual_date end) as slaughter_out_actual_date
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_pipeline_actor_id end) as slaughter_out_actor_id
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_pipeline_status end) as slaughter_out_status
        ,max(case when schedule_type = 'SLAUGHTER_OUT' then last_offsite_storage_id end) as slaugter_out_offsite_storage_id
        ,max(case when schedule_type = 'PACKER_IN' then last_proposed_date end) as packer_in_proposed_date
        ,max(case when schedule_type = 'PACKER_IN' then last_actual_date end) as packer_in_actual_date
        ,max(case when schedule_type = 'PACKER_IN' then last_pipeline_actor_id end) as packer_in_actor_id
        ,max(case when schedule_type = 'PACKER_IN' then last_pipeline_status end) as packer_in_status
        ,max(case when schedule_type = 'PACKER_IN' then last_offsite_storage_id end) as packer_in_offsite_storage_id
        ,max(case when schedule_type = 'PACKER_OUT' then last_proposed_date end) as packer_out_proposed_date
        ,max(case when schedule_type = 'PACKER_OUT' then last_actual_date end) as packer_out_actual_date
        ,max(case when schedule_type = 'PACKER_OUT' then last_pipeline_actor_id end) as packer_out_actor_id
        ,max(case when schedule_type = 'PACKER_OUT' then last_pipeline_status end) as packer_out_status
        ,max(case when schedule_type = 'PACKER_OUT' then last_offsite_storage_id end) as packer_out_offsite_storage_id
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_proposed_date end) as processor_in_proposed_date
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_actual_date end) as processor_in_actual_date
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_pipeline_actor_id end) as processor_in_actor_id
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_pipeline_status end) as processor_in_status
        ,max(case when schedule_type = 'PROCESSOR_IN' then last_offsite_storage_id end) as processor_in_offsite_storage_id
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_proposed_date end) as processor_out_proposed_date
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_actual_date end) as processor_out_actual_date
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_pipeline_actor_id end) as processor_out_actor_id
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_pipeline_status end) as processor_out_status
        ,max(case when schedule_type = 'PROCESSOR_OUT' then last_offsite_storage_id end) as processor_out_offsite_storage_id
        ,max(case when schedule_type = 'FC_IN' then last_proposed_date end) as fc_in_proposed_date
        ,max(case when schedule_type = 'FC_IN' then last_actual_date end) as fc_in_actual_date
        ,max(case when schedule_type = 'FC_IN' then last_pipeline_actor_id end) as fc_in_actor_id
        ,max(case when schedule_type = 'FC_IN' then last_pipeline_status end) as fc_in_status
        ,max(case when schedule_type = 'FC_IN' then last_offsite_storage_id end) as fc_in_offsite_storage_id
        ,max(case when schedule_type = 'FC_SCAN' then last_proposed_date end) as fc_scan_proposed_date
        ,max(case when schedule_type = 'FC_SCAN' then last_actual_date end) as fc_scan_actual_date
        ,max(case when schedule_type = 'FC_SCAN' then last_pipeline_actor_id end) as fc_scan_actor_id
        ,max(case when schedule_type = 'FC_SCAN' then last_pipeline_status end) as fc_scan_status
        ,max(case when schedule_type = 'FC_SCAN' then last_offsite_storage_id end) as fc_scan_offsite_storage_id
    from most_recent_schedule_values
    group by 1
)

,add_actors as (
    select
        pivot_schedule_types.pipeline_order_id
        ,pivot_schedule_types.farm_out_proposed_date
        ,pivot_schedule_types.farm_out_actual_date
        ,pivot_schedule_types.farm_out_status
        ,farm_out_actor.pipeline_actor_name as farm_out_name
        ,farm_out_actor.actor_id as farm_id
        ,pivot_schedule_types.slaughter_kill_proposed_date
        ,pivot_schedule_types.slaughter_kill_actual_date
        ,pivot_schedule_types.slaughter_kill_status
        ,slaughter_kill_actor.pipeline_actor_name as slaughter_kill_name
        ,pivot_schedule_types.slaughter_out_proposed_date
        ,pivot_schedule_types.slaughter_out_actual_date
        ,pivot_schedule_types.slaughter_out_status
        ,slaughter_out_actor.pipeline_actor_name as slaughter_out_name
        ,pivot_schedule_types.packer_in_proposed_date
        ,pivot_schedule_types.packer_in_actual_date
        ,pivot_schedule_types.packer_in_status
        ,packer_in_actor.pipeline_actor_name as packer_in_name
        ,pivot_schedule_types.packer_out_proposed_date
        ,pivot_schedule_types.packer_out_actual_date
        ,pivot_schedule_types.packer_out_status
        ,packer_out_actor.pipeline_actor_name as packer_out_name
        ,pivot_schedule_types.processor_in_proposed_date
        ,pivot_schedule_types.processor_in_actual_date
        ,pivot_schedule_types.processor_in_status
        ,processor_in_actor.pipeline_actor_name as processor_in_name
        ,pivot_schedule_types.processor_out_proposed_date
        ,pivot_schedule_types.processor_out_actual_date
        ,pivot_schedule_types.processor_out_status
        ,pivot_schedule_types.processor_out_offsite_storage_id
        ,processor_out_actor.pipeline_actor_name as processor_out_name
        ,pivot_schedule_types.fc_in_proposed_date
        ,pivot_schedule_types.fc_in_actual_date
        ,pivot_schedule_types.fc_in_status
        ,pivot_schedule_types.fc_in_offsite_storage_id
        ,fc_in_actor.pipeline_actor_name as fc_in_name
        ,fc_in_actor.pipeline_actor_id as fc_in_fc_id
        ,pivot_schedule_types.fc_scan_proposed_date
        ,pivot_schedule_types.fc_scan_actual_date
        ,pivot_schedule_types.fc_scan_status
        ,fc_scan_actor.pipeline_actor_name as fc_scan_name
        ,fc_scan_actor.pipeline_actor_id as fc_scan_fc_id
    from pivot_schedule_types
        left join actor as farm_out_actor on pivot_schedule_types.farm_out_actor_id = farm_out_actor.pipeline_actor_id
        left join actor as slaughter_kill_actor on pivot_schedule_types.slaughter_kill_actor_id = slaughter_kill_actor.pipeline_actor_id
        left join actor as slaughter_out_actor on pivot_schedule_types.slaughter_out_actor_id = slaughter_out_actor.pipeline_actor_id
        left join actor as packer_in_actor on pivot_schedule_types.packer_in_actor_id = packer_in_actor.pipeline_actor_id
        left join actor as packer_out_actor on pivot_schedule_types.packer_out_actor_id = packer_out_actor.pipeline_actor_id
        left join actor as processor_in_actor on pivot_schedule_types.processor_in_actor_id = processor_in_actor.pipeline_actor_id
        left join actor as processor_out_actor on pivot_schedule_types.processor_out_actor_id = processor_out_actor.pipeline_actor_id
        left join actor as fc_in_actor on pivot_schedule_types.fc_in_actor_id = fc_in_actor.pipeline_actor_id
        left join actor as fc_scan_actor on pivot_schedule_types.fc_in_actor_id = fc_scan_actor.pipeline_actor_id
)


select * from add_actors

with

receivable as ( select * from {{ ref('stg_cc__pipeline_receivables') }} )
,pipeline_schedule as ( select * from {{ ref('pipeline_schedules') }} )

,pipeline_receivable as (
    select
        receivable.pipeline_order_id
    
        ,case
            when pipeline_schedule.fc_scan_actual_date is not null then 'COMPLETED'
            when pipeline_schedule.fc_in_offsite_storage_id is not null then 'IN OFFSITE STORAGE'
            when pipeline_schedule.fc_in_actual_date is not null then 'ARRIVED AT FC'
            when pipeline_schedule.processor_out_actual_date is not null then 'IN TRANSIT TO FC'
            when pipeline_schedule.processor_in_actual_date is not null then 'ARRIVED AT PROCESSOR'
            when pipeline_schedule.packer_out_actual_date is not null then 'IN TRANSIT TO PROCESSOR'
            when pipeline_schedule.packer_in_actual_date is not null then 'ARRIVED AT PACKER'
            when pipeline_schedule.slaughter_out_actual_date is not null then 'IN TRANSIT TO PACKER'
            when pipeline_schedule.slaughter_kill_actual_date is not null then 'SLAUGHTERED'
            when pipeline_schedule.farm_out_actual_date is not null then 'IN TRANSIT TO SLAUGHTER'
            else 'ORDER PLACED'
         end as receivable_status
    
        ,pipeline_schedule.fc_scan_fc_id as fc_id
        ,receivable.sku_id
        ,receivable.quantity
        ,receivable.marked_destroyed_at_utc is not null as is_destroyed
        ,receivable.marked_destroyed_at_utc
        ,receivable.created_at_utc
        ,receivable.updated_at_utc
        ,pipeline_schedule.farm_out_proposed_date
        ,pipeline_schedule.farm_out_actual_date
        ,pipeline_schedule.farm_out_status
        ,pipeline_schedule.farm_out_name
        ,pipeline_schedule.farm_id
        ,pipeline_schedule.slaughter_kill_proposed_date
        ,pipeline_schedule.slaughter_kill_actual_date
        ,pipeline_schedule.slaughter_kill_status
        ,pipeline_schedule.slaughter_kill_name
        ,pipeline_schedule.slaughter_out_proposed_date
        ,pipeline_schedule.slaughter_out_actual_date
        ,pipeline_schedule.slaughter_out_status
        ,pipeline_schedule.slaughter_out_name
        ,pipeline_schedule.packer_in_proposed_date
        ,pipeline_schedule.packer_in_actual_date
        ,pipeline_schedule.packer_in_status
        ,pipeline_schedule.packer_in_name
        ,pipeline_schedule.packer_out_proposed_date
        ,pipeline_schedule.packer_out_actual_date
        ,pipeline_schedule.packer_out_status
        ,pipeline_schedule.packer_out_name
        ,pipeline_schedule.processor_in_proposed_date
        ,pipeline_schedule.processor_in_actual_date
        ,pipeline_schedule.processor_in_status
        ,pipeline_schedule.processor_in_name
        ,pipeline_schedule.processor_out_proposed_date
        ,pipeline_schedule.processor_out_actual_date
        ,pipeline_schedule.processor_out_status
        ,pipeline_schedule.processor_out_offsite_storage_id
        ,pipeline_schedule.processor_out_name
        ,pipeline_schedule.fc_in_proposed_date
        ,pipeline_schedule.fc_in_actual_date
        ,pipeline_schedule.fc_in_status
        ,pipeline_schedule.fc_in_offsite_storage_id
        ,pipeline_schedule.fc_in_name
        ,pipeline_schedule.fc_in_fc_id
        ,pipeline_schedule.fc_scan_proposed_date
        ,pipeline_schedule.fc_scan_actual_date
        ,pipeline_schedule.fc_scan_status
        ,pipeline_schedule.fc_scan_name
    from receivable
        left join pipeline_schedule on receivable.pipeline_order_id = pipeline_schedule.pipeline_order_id
)

select * from pipeline_receivable

with 

source as ( select * from {{ source('zendesk', 'ticket') }} )

,renamed as (
    select
        id as ticket_id
        ,{{ clean_strings('url') }} as ticket_url
        ,external_id
        ,{{ clean_strings('via_channel') }} as via_channel
        ,via_source_from_id
        ,{{ clean_strings('via_source_from_title') }} as via_source_from_title
        ,{{ clean_strings('via_source_from_address') }} as via_source_from_email
        ,{{ clean_strings('via_source_to_name') }} as via_source_to_name
        ,{{ clean_strings('via_source_to_address') }} as via_source_to_email
        ,{{ clean_strings('via_source_rel') }} as via_source_rel
        ,merged_ticket_ids
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,{{ clean_strings('type') }} as ticket_type
        ,{{ clean_strings('subject') }} as ticket_subject
        ,{{ clean_strings('description') }} as ticket_description
        ,{{ clean_strings('priority') }} as ticket_priority
        ,{{ clean_strings('status') }} as ticket_status
        ,{{ clean_strings('recipient') }} as recipeint_email
        ,requester_id
        ,submitter_id
        ,assignee_id
        ,organization_id
        ,group_id
        ,forum_topic_id
        ,problem_id
        ,has_incidents
        ,is_public
        ,via_followup_source_id
        ,followup_ids
        ,due_at as due_at_utc
        ,ticket_form_id
        ,brand_id
        ,allow_channelback as does_allow_channelback
        ,{{ clean_strings('system_client') }} as system_client
        ,system_raw_email_identifier
        ,system_json_email_identifier
        ,system_message_id
        ,system_ip_address
        ,{{ clean_strings('custom_order_errors') }} as order_errors
        ,custom_playlist_assigned_at
        ,{{ clean_strings('custom_issue_category_') }} as issue_category
        ,{{ clean_strings('custom_cow_cash_reason') }} as cow_cash_reason
        ,{{ clean_strings('custom_ai_category') }} as ai_category
        ,{{ clean_strings('custom_filter_for_productivity') }} as filter_for_productivity
        ,custom_order_id as order_token
        ,custom_time_spent_last_update_sec_ as time_spent_last_update_seconds
        ,{{ clean_strings('custom_automation_type') }} as automation_type
        ,custom_playlist_assigned_ as playlist_assigned
        ,custom_playlist_assigned_by as playlist_assigned_by
        ,custom_total_time_spent_sec_ as total_time_spent_seconds
        ,custom_total_credit_refund_given as total_credit_refund_given
        ,{{ clean_strings('custom_type') }} as custom_type
        ,custom_playlist_autoplayed_ as playlist_autoplayed
        ,_fivetran_synced
        ,{{ clean_strings('system_location') }} as system_location
        ,system_latitude
        ,system_longitude
        ,system_machine_generated as is_machine_generated
        ,system_ccs
        ,custom_blastable_potential_template as is_potential_blastable_template
        ,{{ clean_strings('custom_ticket_form_name') }} as ticket_form_name
        ,custom_ticket_form_order_number as ticket_form_order_number
        ,{{ clean_strings('custom_ticket_form_reason') }} as ticket_form_reason
        ,system_email_id
        ,system_eml_redacted
    from source
)

select * from renamed
